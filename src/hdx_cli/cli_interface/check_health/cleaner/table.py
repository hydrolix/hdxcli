import logging

from .. import const, reportlog, utils
from . import transforms, views


class TableCleaner:
    """Load with an auto-view and then run transforms through it to find issues"""

    def __init__(
        self,
        table: dict = {},
        transforms: list[dict] = [],
        auto_view: dict = {},
    ) -> None:
        if (not auto_view) and (not transforms):
            return
        self.table = table
        self.raw_auto_view = auto_view
        self.raw_transforms = transforms
        self._process()

    def _process(self):
        """Process the transforms and views"""
        self.column_reference = {}
        self.auto_view = None
        if self.raw_auto_view:
            self.auto_view = views.AutoView(self.raw_auto_view)
            self.column_reference = self.auto_view.column_reference
        self.transforms = []
        for t in self.raw_transforms:
            transform = transforms.Transform(t, self.column_reference)
            self.column_reference.update(transform.new_columns)
            self.transforms.append(transform)

    # PROPERTIES
    @property
    def table_name(self):
        """What is the name of this table?"""
        return self.table.get(const.FIELD_NAME, "")

    @property
    def repair_is_possible(self):
        """Is it possible to repair this table?"""
        if not self.auto_view:
            return False
        autoview_repair_possible = self.auto_view.repair_is_possible
        transform_repair_possible = all([t.repair_is_possible for t in self.transforms])
        primary_ok = self._primary_column_count() == 1
        return all(
            [
                autoview_repair_possible,
                transform_repair_possible,
                primary_ok,
            ]
        )

    @property
    def repair_is_necessary(self):
        """Is it necessary to repair this table?"""
        if not self.auto_view:
            return True
        autoview_repair_necessary = self.auto_view.repair_is_necessary
        transform_repair_necessary = any(
            [t.repair_is_necessary for t in self.transforms]
        )
        primary_repair_necessary = self._primary_column_count() != 1
        return any(
            [
                autoview_repair_necessary,
                transform_repair_necessary,
                primary_repair_necessary,
            ]
        )

    def print_reports(self):
        """print out the complete report for this table"""

        # Global report
        report = reportlog.ReportLog()
        if not self.raw_auto_view:
            report.add_message(logging.WARNING, "Table has no auto_view")
        if not self.raw_transforms:
            report.add_message(logging.WARNING, "Table has no transforms")
        primary_column_count = self._primary_column_count()
        if primary_column_count != 1:
            report.add_message(
                logging.CRITICAL,
                f"Table has {primary_column_count} primary columns. Should have exactly 1",
            )
        report.print_report()

        # autoview report
        if self.auto_view:
            utils.print_header(f"{self.table_name} - auto_view")
            self.auto_view.print_report()

        # transform reports
        for transform in self.transforms:
            utils.print_header(f"{self.table_name} - {transform.name}")
            transform.print_report()

    @property
    def corrected_autoview(self):
        """Return a correct autoview if possible"""
        if not self.auto_view:
            return {}
        if self.repair_is_possible:
            if self.auto_view.repair_is_necessary:
                return self.auto_view.corrected_schema

    @property
    def corrected_transforms(self):
        """Return correct transforms if possible"""
        transforms_to_repair = {}
        if self.repair_is_possible:
            for transform in self.transforms:
                if transform.repair_is_necessary:
                    transforms_to_repair[transform.id] = transform.corrected_schema
        return transforms_to_repair

    def _primary_column_count(self):
        """How many primary columns does this table have?"""
        if not self.auto_view:
            return 0
        primary_column_names = set([c.name for c in self.auto_view.primary_columns])
        for transform in self.transforms:
            primary_column_names = primary_column_names.union(
                [c.name for c in transform.primary_columns]
            )
        return len(primary_column_names)
