FIELD_TYPE = "type"
FIELD_UUID = "uuid"
FIELD_RESOLUTION = "resolution"
FIELD_NAME = "name"
FIELD_INDEX = "index"
FIELD_PRIMARY = "primary"
FIELD_ELEMENTS = "elements"
FIELD_SETTINGS = "settings"
FIELD_OUTPUT_COLUMNS = "output_columns"
FIELD_DATATYPE = "datatype"
FIELD_SUPPRESS = "suppress"
FIELD_IGNORE = "ignore"
FIELD_MODIFIED = "modified"

SKIP_FIELDS = [FIELD_SUPPRESS, FIELD_IGNORE]

TYPE_STRING = "string"
TYPE_BOOL = "bool"
TYPE_BOOLEAN = "boolean"
TYPE_UINT8 = "uint8"
TYPE_UINT16 = "uint16"
TYPE_UINT32 = "uint32"
TYPE_UINT64 = "uint64"
TYPE_INT8 = "int8"
TYPE_INT16 = "int16"
TYPE_INT32 = "int32"
TYPE_INT64 = "int64"
TYPE_JSON = "json"
TYPE_ARRAY = "array"
TYPE_MAP = "map"
TYPE_DATETIME = "datetime"
TYPE_DATETIME64 = "datetime64"
TYPE_EPOCH = "epoch"


AUTO_VIEW_NAME = "auto_view"

BOOL_TYPES = (TYPE_BOOL, TYPE_BOOLEAN)
MAP_TYPES = (TYPE_ARRAY, TYPE_MAP)
DATETIME_TYPES = (TYPE_DATETIME, TYPE_DATETIME64, TYPE_EPOCH)
COMPLEX_TYPES = (TYPE_ARRAY, TYPE_MAP)

RESOLUTION_SECOND = "s"
RESOLUTION_MILLISECOND = "ms"

RESOLUTION_MAP = {
    "milliseconds": "ms",
    "millisecond": "ms",
    "millis": "ms",
    "milli": "ms",
    "ms": "ms",
    "seconds": "s",
    "second": "s",
    "secs": "s",
    "sec": "s",
    "s": "s",
}

INDEXABLE_IN_MAP_TYPES = [
    TYPE_STRING,
    TYPE_BOOL,
    TYPE_BOOLEAN,
    TYPE_UINT8,
    TYPE_UINT16,
    TYPE_UINT32,
    TYPE_UINT64,
    TYPE_INT8,
    TYPE_INT16,
    TYPE_INT32,
    TYPE_INT64,
    TYPE_JSON,
]
ALWAYS_INDEXABLE_TYPES = INDEXABLE_IN_MAP_TYPES + [
    TYPE_DATETIME,
    TYPE_DATETIME64,
    TYPE_EPOCH,
]
COMPLEX_INDEXABLE_SUBTYPES = {
    TYPE_ARRAY: ALWAYS_INDEXABLE_TYPES,
    TYPE_MAP: INDEXABLE_IN_MAP_TYPES,
}
