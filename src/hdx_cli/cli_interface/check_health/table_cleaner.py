import logging
from operator import itemgetter

from . import const, reportlog, utils


class TableCleaner:
    """Load with an auto-view and then run transforms through it to find issues"""

    def __init__(
        self,
        table: dict = {},
        transforms: list[dict] = [],
        views: list[dict] = [],
    ) -> None:
        self.table = table
        self.transforms = transforms
        self.all_views = views
        self.auto_view = self._get_auto_view()
        self.columns = {}

        if self.auto_view:
            self.auto_view_column_names = self._auto_view_column_names()
        if self.has_transforms or self.has_views:
            self.columns = self._build_correct_columns()

    @property
    def has_transforms(self):
        """Does this table have transforms"""
        return bool(self.transforms)

    @property
    def has_views(self):
        """Does this table have views"""
        return bool(self.all_views)

    @property
    def table_name(self):
        """What is the name of this table?"""
        return self.table.get(const.FIELD_NAME, "")

    @staticmethod
    def sort_by_modified(unsorted_list: list = []):
        """Sort a list of dictionaries by their modified property"""
        return sorted(unsorted_list, key=itemgetter(const.FIELD_MODIFIED), reverse=True)

    @classmethod
    def get_view_datatype(cls, type: str, resolution: str | None = None) -> str:
        """Normalize type on transform column to match view types"""
        if type in const.BOOL_TYPES:
            return const.TYPE_UINT8
        if type in const.DATETIME_TYPES:
            if resolution == const.RESOLUTION_MILLISECOND:
                return const.TYPE_DATETIME64
            else:
                return const.TYPE_DATETIME
        return type

    @classmethod
    def normalize_elements(cls, elements: list) -> list:
        """Normalize elements on complex type"""
        normalized_elements = []
        for element in elements:
            normalized_element = {}
            normalized_type = cls.get_view_datatype(
                element.get(const.FIELD_TYPE, None),
                element.get(const.FIELD_RESOLUTION, None),
            )
            normalized_element[const.FIELD_TYPE] = normalized_type
            if normalized_type in const.DATETIME_TYPES:
                normalized_resolution = const.RESOLUTION_MAP.get(
                    element.get(const.FIELD_RESOLUTION), None
                )
                if normalized_resolution is None:
                    if normalized_type == const.TYPE_DATETIME64:
                        normalized_resolution = const.RESOLUTION_MILLISECOND
                    else:
                        normalized_resolution = const.RESOLUTION_SECOND
                element[const.FIELD_RESOLUTION] = normalized_resolution
            elif normalized_type in const.COMPLEX_TYPES:
                sub_elements = element.get(const.FIELD_ELEMENTS, [])
                normalized_sub_elements = cls.normalize_elements(sub_elements)
                if not normalized_sub_elements:
                    # A complex type without elements is invalid
                    continue
                normalized_element[const.FIELD_ELEMENTS] = normalized_sub_elements
            normalized_elements.append(normalized_element)
        return normalized_elements

    @classmethod
    def is_column_indexable(cls, datatype):
        """Is this column allowed to have index True?"""
        primary = datatype.get(const.FIELD_PRIMARY, False)
        if primary:
            return False

        type = datatype.get(const.FIELD_TYPE, None)
        if type in const.ALWAYS_INDEXABLE_TYPES:
            return True
        if type in const.COMPLEX_TYPES:
            elements = datatype.get(const.FIELD_ELEMENTS, [])
            element_types = set(
                [
                    e.get(const.FIELD_TYPE, None)
                    for e in elements
                    if e.get(const.FIELD_TYPE, None) is not None
                ]
            )
            acceptable_subtypes = set(const.COMPLEX_INDEXABLE_SUBTYPES[type])
            if element_types <= acceptable_subtypes:
                return True
        return False

    @classmethod
    def correct_one_column(cls, column={}):
        """Take one column and normalize it for comparison"""
        correct_column = {}

        name = column.get(const.FIELD_NAME, "")
        datatype = column.get(const.FIELD_DATATYPE, {})

        should_skip = False
        for field in const.SKIP_FIELDS:
            should_skip = datatype.get(field, False)

        if should_skip or (not name) or (not datatype):
            return correct_column
        correct_column[const.FIELD_NAME] = name

        # type
        type = datatype.get(const.FIELD_TYPE, None)
        resolution = datatype.get(const.FIELD_RESOLUTION, None)
        resolution = const.RESOLUTION_MAP.get(resolution, None)
        correct_type = cls.get_view_datatype(type, resolution)
        correct_column[const.FIELD_TYPE] = correct_type

        # index
        index = False
        is_indexable = cls.is_column_indexable(datatype)
        if is_indexable:
            index = datatype.get(const.FIELD_INDEX, True)
        correct_column[const.FIELD_INDEX] = index

        primary = False
        if type in const.DATETIME_TYPES:
            primary = datatype.get(const.FIELD_PRIMARY, False)
            if resolution is None:
                if type == const.TYPE_DATETIME64:
                    resolution = const.RESOLUTION_MILLISECOND
                else:
                    resolution = const.RESOLUTION_SECOND
            correct_column[const.FIELD_RESOLUTION] = resolution
        correct_column[const.FIELD_PRIMARY] = primary

        elements = None
        if type in const.COMPLEX_TYPES:
            elements = cls.normalize_elements(datatype.get(const.FIELD_ELEMENTS, []))
            correct_column[const.FIELD_ELEMENTS] = elements

        # name, type, index, primary, elements
        return correct_column

    def _get_auto_view(self):
        """Search through the views and find the auto_view if it exists"""
        report_log = reportlog.ReportLog()
        auto_view = None
        for view in self.all_views:
            view_name = view.get(const.FIELD_NAME, "")
            if view_name == const.AUTO_VIEW_NAME:
                if auto_view is None:
                    auto_view = view
                else:
                    report_log.add_message(
                        logging.CRITICAL, "More than one auto_view found on table."
                    )
        if auto_view is None:
            report_log.add_message(logging.CRITICAL, "Table has no auto_view")
        report_log.report()
        return auto_view

    def _transform_column_names(self):
        """Get the names of all non-skip transform columns"""

    def _auto_view_column_names(self):
        """Get the names of all auto_view columns"""
        if not self.auto_view:
            return []
        auto_view_settings = self.auto_view.get(const.FIELD_SETTINGS, {}) or {}
        auto_view_columns = auto_view_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
        auto_view_column_names = set([c.get("name") for c in auto_view_columns])
        return [c for c in auto_view_column_names if c is not None]

    @classmethod
    def normalize_columns(cls, raw_columns=[]):
        """normalize a set of columns from a transform or view"""
        correct_columns = {}
        for column in raw_columns:
            correct_column = cls.correct_one_column(column)
            column_name = correct_column.pop("name", None)
            if column_name:
                correct_columns[column_name] = correct_column
        return correct_columns

    def _build_correct_columns(self):
        """Determine the correct types for each column"""
        if self.auto_view is None:
            return
        # Start with the auto_view columns
        auto_view_settings = self.auto_view.get(const.FIELD_SETTINGS, {}) or {}
        auto_view_columns = auto_view_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
        correct_columns = self.normalize_columns(auto_view_columns)
        sorted_transforms = self.sort_by_modified(self.transforms)
        # If a transform has a column that didn’t make it to the auto-view, add it in
        for transform in sorted_transforms:
            transform_settings = transform.get(const.FIELD_SETTINGS, {})
            transform_columns = transform_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
            normalized_transform_columns = self.normalize_columns(transform_columns)
            for column_name, transform_column in normalized_transform_columns.items():
                if column_name not in correct_columns:
                    correct_columns[column_name] = transform_column
        return correct_columns

    def _check_primary(self):
        """Check if table has exactly one primary column"""
        report_log = reportlog.ReportLog()
        primary_columns = []
        if self.columns:
            for column_name, column_values in self.columns.items():
                if column_values.get("primary", False):
                    primary_columns.append(column_name)
        if len(primary_columns) == 0:
            report_log.add_message(logging.ERROR, "Table has no primary columns")
        elif len(primary_columns) > 1:
            report_log.add_message(
                logging.CRITICAL,
                f"Table has more than one primary column — '{primary_columns}'",
            )
        report_log.report()

    def _column_report(self, column) -> bool:
        """analyze a single column
        Return true if column is clean, false otherwise"""
        report_log = reportlog.ReportLog()
        # Get the name
        clean = True
        name = column.get(const.FIELD_NAME, None)
        if not name:
            report_log.add_message(logging.ERROR, "Column has no name")
            clean = False
            report_log.report()
            return clean
        # Get the datatype
        datatype = column.get(const.FIELD_DATATYPE, {})
        if not datatype:
            report_log.add_message(logging.ERROR, f"Column '{name}' has no datatype")
            clean = False
            report_log.report()
            return clean
        # Should we skip this column?
        should_skip = False
        for field in const.SKIP_FIELDS:
            should_skip = datatype.get(field, False)
        if should_skip:
            report_log.report()
            return clean
        # Check if it appears in the auto-view
        if name not in self.auto_view_column_names:
            report_log.add_message(
                logging.WARNING, f"Column '{name}' not present in auto_view"
            )
        # Get the correct column definition
        if not self.columns:
            report_log.report()
            return clean
        correct_column = self.columns.get(name, None)
        if correct_column is None:
            report_log.add_message(logging.ERROR, f"Column '{name}' not found")
            clean = False
            report_log.report()
            return clean
        # Check for type conflict
        transform_type = datatype.get(const.FIELD_TYPE, None)
        if not transform_type:
            report_log.add_message(logging.ERROR, f"Column '{name}' has no type")
            clean = False
        resolution = datatype.get(const.FIELD_RESOLUTION, None)
        view_type = self.get_view_datatype(transform_type, resolution)
        correct_type = correct_column.get(const.FIELD_TYPE)
        if view_type != correct_type:
            report_log.add_message(
                logging.ERROR,
                f"Column '{name}' type '{view_type}' conflicts with correct type '{correct_type}'",
            )
            clean = False
        # Check for index conflict
        index = datatype.get(const.FIELD_INDEX, None)
        is_indexable = self.is_column_indexable(datatype)
        if index and not is_indexable:
            report_log.add_message(
                logging.ERROR,
                f"Column '{name}' index '{index}', but is not indexable",
            )
        correct_index = correct_column.get(const.FIELD_INDEX, None)
        if index != correct_index:
            report_log.add_message(
                logging.ERROR,
                f"Column '{name}' index '{index}' conflicts with correct index '{correct_index}'",
            )
            clean = False
        # Check for primary conflict
        is_datetime = (view_type in const.DATETIME_TYPES) or (
            correct_type in const.DATETIME_TYPES
        )
        primary = datatype.get(const.FIELD_PRIMARY, False)
        correct_primary = correct_column.get(const.FIELD_PRIMARY, False)
        if primary != correct_primary:
            report_log.add_message(
                logging.ERROR,
                f"Column '{name}' primary '{primary}' conflicts with correct primary '{correct_primary}'",
            )
            clean = False
        if primary and not is_datetime:
            report_log.add_message(
                logging.ERROR,
                f"Column '{name}' primary '{primary}', but is not a datetime type",
            )
            clean = False

        # Check for resolution conflict on datetimes only
        if is_datetime:
            resolution = const.RESOLUTION_MAP.get(
                datatype.get(const.FIELD_RESOLUTION), None
            )
            correct_resolution = correct_column.get(const.FIELD_RESOLUTION, None)
            if resolution != correct_resolution:
                report_log.add_message(
                    logging.ERROR,
                    f"Column '{name}' resolution '{resolution}' conflicts with correct resolution '{correct_resolution}'",
                )
                clean = False

        # Check for elements conflict
        view_type_complex = view_type in const.COMPLEX_TYPES
        correct_type_complex = correct_type in const.COMPLEX_TYPES
        is_complex = view_type_complex or correct_type_complex
        if is_complex:
            elements = datatype.get(const.FIELD_ELEMENTS, [])
            normalized_elements = self.normalize_elements(elements)
            correct_elements = correct_column.get(const.FIELD_ELEMENTS, [])
            if normalized_elements != correct_elements:
                report_log.add_message(
                    logging.ERROR,
                    f"Column '{name}' elements '{elements}' conflicts with correct elements '{correct_elements}",
                )
                clean = False
        report_log.report()
        return clean

    def _check_auto_view_columns(self):
        report_log = reportlog.ReportLog()
        transform_column_names = set()
        transform_skipped_columns = set()
        for transform in self.transforms:
            transform_settings = transform.get(const.FIELD_SETTINGS, {})
            transform_columns = transform_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
            for column in transform_columns:
                name = column.get("name", "")
                if not name:
                    continue
                datatype = column.get(const.FIELD_DATATYPE, {})
                should_skip = False
                for field in const.SKIP_FIELDS:
                    should_skip = datatype.get(field, False)
                if should_skip:
                    transform_skipped_columns.add(name)
                    continue
                transform_column_names.add(name)
        for name in self.auto_view_column_names:
            if name not in transform_column_names:
                if name in transform_skipped_columns:
                    report_log.add_message(
                        logging.WARNING,
                        "Field '{name}' in auto_view, but suppressed/ignored in transform",
                    )
                else:
                    report_log.add_message(
                        logging.WARNING,
                        f"Field '{name}' in auto_view, but not in any transforms",
                    )
        report_log.report()

    def _auto_view_report(self):
        report_log = reportlog.ReportLog()
        utils.print_header(f"{self.table_name} — auto_view", underline_char="—")
        if not self.auto_view:
            report_log.add_message(logging.CRITICAL, "Table has no auto-view")
        else:
            auto_view_settings = self.auto_view.get(const.FIELD_SETTINGS, {}) or {}
            auto_view_columns = auto_view_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
            self._check_auto_view_columns()
            auto_view_is_clean = True
            for column in auto_view_columns:
                column_is_clean = self._column_report(column)
                auto_view_is_clean = auto_view_is_clean and column_is_clean
            if auto_view_is_clean:
                report_log.add_message(logging.INFO, "Autoview is correct")
        report_log.report()

    def _report_all_transforms(self):
        """check all transforms"""
        for transform in self.transforms:
            report_log = reportlog.ReportLog()
            transform_name = transform.get(const.FIELD_NAME, "")
            utils.print_header(
                f"{self.table_name} — {transform_name}", underline_char="—"
            )
            transform_settings = transform.get(const.FIELD_SETTINGS, {})
            transform_columns = transform_settings.get(const.FIELD_OUTPUT_COLUMNS, [])
            transform_is_clean = True
            for column in transform_columns:
                column_is_clean = self._column_report(column)
                transform_is_clean = transform_is_clean and column_is_clean
            if transform_is_clean:
                report_log.add_message(logging.INFO, "Transform is correct")
            report_log.report()

    def table_report(self):
        """Print a report of all table issues"""
        report_log = reportlog.ReportLog()
        utils.print_header(self.table_name)
        if not self.columns:
            report_log.add_message(logging.INFO, "Table has no views or transforms")
            report_log.report()
            return
        self._check_primary()
        self._auto_view_report()
        self._report_all_transforms()
