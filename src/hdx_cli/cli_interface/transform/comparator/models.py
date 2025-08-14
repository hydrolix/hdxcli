from __future__ import annotations
import re

def _normalize_value(value):
    """Recursively normalizes a value for stable comparison.
    If the value is a model instance, it asks for its comparable dictionary."""
    if hasattr(value, 'as_comparable_dict'):
        return value.as_comparable_dict()
    if isinstance(value, list):
        # Sort lists of dictionaries to ensure order doesn't affect comparison
        if all(isinstance(i, dict) for i in value):
            return [_normalize_value(item) for item in value]
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        # Return a new dict with sorted keys and normalized values for consistency
        return {k: _normalize_value(v) for k, v in sorted(value.items())}
    return value

class ComparableElement:
    """Represents a single, potentially nested, 'element'."""
    def __init__(self, raw_element: dict):
        self._data = raw_element if raw_element else {}
        # Recursively create models for any sub-elements
        self._elements = [ComparableElement(e) for e in self._data.get("elements", [])]

    def as_comparable_dict(self) -> dict:
        """Converts this element into a clean dictionary for comparison."""
        comparable = self._data.copy()
        if self._elements:
            # Replace raw elements with their normalized dictionary representation
            comparable['elements'] = [elem.as_comparable_dict() for elem in self._elements]
        return _normalize_value(comparable)

class ComparableColumn:
    """Represents a full 'output_column' with its nested datatype."""
    def __init__(self, raw_column: dict):
        self._data = raw_column if raw_column else {}
        self.name = self._data.get("name", "")
        self._datatype = self._data.get("datatype", {})
        self._elements = [ComparableElement(e) for e in self._datatype.get("elements", [])]

    def as_comparable_dict(self) -> dict:
        """Generates a dictionary of the column's 'datatype' for comparison."""
        comparable_datatype = self._datatype.copy()
        if self._elements:
            comparable_datatype['elements'] = [elem.as_comparable_dict() for elem in self._elements]
        return _normalize_value(comparable_datatype)

    def __eq__(self, other: object) -> bool:
        """Equality is determined by comparing their dictionary representations."""
        if not isinstance(other, ComparableColumn):
            return NotImplemented
        return self.as_comparable_dict() == other.as_comparable_dict()

class ComparableTransform:
    """A wrapper for a raw transform JSON, providing clean access to properties."""
    def __init__(self, raw_data: dict, description: str):
        self._data = raw_data if raw_data else {}
        self.description = description

    @property
    def name(self) -> str:
        return self._data.get("name", "N/A")

    @property
    def type(self) -> str:
        return self._data.get("type", "N/A")

    @property
    def settings(self) -> dict:
        """Returns the complete 'settings' object, normalized for comparison."""
        raw_settings = self._data.get("settings", {}).copy()
        # sql_transform and output_columns are handled separately
        raw_settings.pop("sql_transform", None)
        raw_settings.pop("output_columns", None)
        return _normalize_value(raw_settings)

    @property
    def sql(self) -> str | None:
        """Returns a normalized version of the SQL transform.
        This is a simple cleanup, not a full parse."""
        raw_sql = self._data.get("settings", {}).get("sql_transform")
        if not raw_sql:
            return None
        # Remove multi-line comments, then single-line, collapse whitespace, and trim.
        no_multiline = re.sub(r'/\*.*?\*/', '', raw_sql, flags=re.DOTALL)
        no_singleline = re.sub(r'--.*?\n', '', no_multiline)
        single_space = re.sub(r'\s+', ' ', no_singleline)
        return single_space.strip().lower()

    @property
    def output_columns(self) -> dict[str, ComparableColumn]:
        """Returns a dictionary of column names to their ComparableColumn models."""
        columns_data = self._data.get("settings", {}).get("output_columns", [])
        return {col.get("name"): ComparableColumn(col) for col in columns_data if col.get("name")}
