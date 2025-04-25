import collections

from .. import const, reportlog
from . import columns


class TableSchema:
    def __init__(self, schema: dict = {}) -> None:
        self.schema = schema
        settings = schema.get(const.FIELD_SETTINGS, {})
        self.raw_columns = settings.get(const.FIELD_OUTPUT_COLUMNS, [])
        self.columns = [columns.Column(**c) for c in self.raw_columns]

    @property
    def id(self) -> str:
        return self.schema.get(const.FIELD_UUID, "")

    @property
    def unskipped_columns(self) -> list[columns.Column]:
        """Columns that are not suppressed or ignored"""
        return [c for c in self.columns if not c.skip]

    @property
    def column_names(self) -> set[str]:
        """get the names of all columns"""
        return set([c.name for c in self.unskipped_columns if not c.skip])

    @property
    def primary_columns(self) -> list[columns.Column]:
        """Get the names of primary columns"""
        return [c for c in self.unskipped_columns if c.primary]

    @property
    def has_repeat_columns(self) -> bool:
        """Are there any repeated column names?"""
        counter = collections.Counter(
            [c.name for c in self.unskipped_columns if not c.skip]
        )
        for name, count in counter.items():
            if count > 1:
                return True
        return False

    @property
    def repair_is_possible(self) -> bool:
        """Can we repair this?"""
        if self.has_repeat_columns:
            return False
        if len(self.primary_columns) != 1:
            return False
        return all([c.repair_is_possible for c in self.unskipped_columns])

    @property
    def repair_is_necessary(self) -> bool:
        """Does this need repair?"""
        if self.has_repeat_columns:
            return True
        if len(self.primary_columns) != 1:
            return True
        return any([c.repair_is_necessary for c in self.unskipped_columns])

    @property
    def corrected_schema(self) -> dict:
        """Produce a version of this with errors corrected"""
        correct_output_columns = [c.corrected_column for c in self.unskipped_columns]
        corrected_schema = self.schema
        corrected_schema["settings"]["output_columns"] = correct_output_columns
        return corrected_schema

    def print_report(self) -> None:
        """Print a report of issues on this tableschema"""
        report = reportlog.ReportLog()
        for column in self.unskipped_columns:
            report = column.build_report(report)
        report.print_report()
