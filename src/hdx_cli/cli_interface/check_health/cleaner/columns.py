import logging

from .. import const, reportlog
from . import elements


class Column:
    def __init__(self, raw_column: dict) -> None:
        self.raw_column = raw_column
        self.name = self.raw_column.get("name", "")
        self.datatype = self.raw_column.get("datatype", {})
        self.column_type = self.datatype.get(const.FIELD_TYPE, None)
        self.index = self.datatype.get(const.FIELD_INDEX, None)
        self.primary = self.datatype.get(const.FIELD_PRIMARY, False)
        self.raw_resolution = self.datatype.get(const.FIELD_RESOLUTION, None)
        self.raw_elements = self.datatype.get(const.FIELD_ELEMENTS, [])
        self.elements = elements.Elements(self.column_type, self.raw_elements)
        self.suppress = self.datatype.get(const.FIELD_SUPPRESS, {})
        self.ignore = self.datatype.get(const.FIELD_IGNORE, {})

    @property
    def skip(self) -> bool:
        """Should this column be skipped?"""
        return self.suppress or self.ignore

    @property
    def repair_is_possible(self) -> bool:
        """Is repairing this column possible?"""
        return False

    @property
    def repair_is_necessary(self) -> bool:
        """Is repairing this column necessary?"""
        return False

    @property
    def conflict_reference(self) -> dict:
        """Return a version of this column that can be used as a conflict reference"""
        return {}

    @property
    def corrected_column(self) -> dict:
        """Return a corrected version of this column"""
        return {}

    @property
    def element_types(self) -> set:
        """Get the types of all elements"""
        retval = set()
        for element in self.elements.elements:
            if element.element_type:
                retval.add(element.element_type)
        return retval

    @property
    def is_indexable(self) -> bool:
        """Can this column be indexed?"""
        if self.primary:
            return False
        if self.column_type in const.ALWAYS_INDEXABLE_TYPES:
            return True
        if self.column_type in const.COMPLEX_TYPES:
            acceptable_subtypes = set(
                const.COMPLEX_INDEXABLE_SUBTYPES[self.column_type]
            )
            if self.element_types <= acceptable_subtypes:
                return True
        return False

    @property
    def resolution(self) -> str | None:
        """Get the normalized resolution of this column, default to second"""
        return const.RESOLUTION_MAP.get(self.raw_resolution)

    @property
    def can_be_primary(self) -> bool:
        """Can this column be primary?"""
        return self.column_type in const.DATETIME_TYPES

    ## REPORT
    def _type_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report issues with the type field"""
        if self.column_type not in const.ALL_TYPES:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', which is not a valid type",
            )
        return report

    def _primary_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if self.primary:
            if not self.can_be_primary:
                report.add_message(
                    logging.ERROR,
                    f"Column '{self.name}' primary enabled, but can’t be primary",
                )
        return report

    def _index_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report issues with the index field"""
        if not self.is_indexable:
            if self.index:
                report.add_message(
                    logging.ERROR,
                    f"Column '{self.name}' index enabled, but not indexable",
                )
        return report

    def _resolution_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report issues with the resolution field"""
        if self.column_type not in const.VIEW_DATETIME_TYPES:
            return report
        if not self.resolution:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', resolution is required, but not set",
            )
            return report
        is_64 = self.column_type == const.TYPE_DATETIME64
        is_ms = self.resolution == const.RESOLUTION_MILLISECOND
        if is_64 and not is_ms:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', but resolution is not 'ms'",
            )
        elif not is_64 and is_ms:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', but resolution is not 's'",
            )

        return report

    def _element_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if self.column_type not in const.COMPLEX_TYPES:
            return report
        report = self.elements.report(report)
        return report

    def build_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Generate a report of issues on this tableschema"""
        report_functions = [
            self._type_report,
            self._primary_report,
            self._index_report,
            self._resolution_report,
            self._element_report,
        ]
        for report_function in report_functions:
            report = report_function(report)
        return report
