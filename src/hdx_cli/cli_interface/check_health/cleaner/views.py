from .. import const
from . import schemas, view_columns


class AutoView(schemas.TableSchema):
    """A single auto_view"""

    def __init__(self, schema: dict = {}):
        self.schema = schema
        self.settings = schema.get(const.FIELD_SETTINGS, {})
        self.raw_columns = self.settings.get(const.FIELD_OUTPUT_COLUMNS, [])
        self.columns = [view_columns.AutoViewColumn(c) for c in self.raw_columns]

    @property
    def column_reference(self) -> dict:
        """Produce a lookup table of the columns on here for conflict checking purposes"""
        reference = {}
        for column in self.unskipped_columns:
            reference[column.name] = column.conflict_reference
        return reference
