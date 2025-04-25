from .. import const
from . import schemas, transform_columns


class Transform(schemas.TableSchema):
    def __init__(self, schema: dict = {}, column_reference: dict = {}) -> None:
        self.schema = schema
        self.settings = schema.get(const.FIELD_SETTINGS, {})
        self.raw_columns = self.settings.get(const.FIELD_OUTPUT_COLUMNS, [])
        self.column_reference = column_reference
        self._build_columns()

    def _build_columns(self):
        """Initialize the transform columns"""
        self.columns = []
        for raw_column in self.raw_columns:
            name = raw_column.get(const.FIELD_NAME, None)
            reference_column = self.column_reference.get(name, None) if name else None
            if reference_column:
                self.columns.append(
                    transform_columns.TransformColumn(raw_column, reference_column)
                )
            else:
                self.columns.append(transform_columns.TransformColumn(raw_column))

    @property
    def name(self) -> str | None:
        return self.schema.get(const.FIELD_NAME, "")

    @property
    def new_columns(self) -> dict:
        # Return the transform columns that are not in the reference
        new_columns = {}
        for c in self.unskipped_columns:
            if c.name not in self.column_reference:
                new_columns[c.name] = c.corrected_column
        return new_columns
