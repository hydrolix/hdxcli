from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional, Set, Union, Dict


__all__ = ['ColumnDefinition',
           'DdlCreateTableInfo',
           'DdlTypeToHdxTypeMappingFunc',
           'IngestIndexError',
           'NoDdlMappingFoundError']

DdlTypeToHdxTypeMappingFunc = Callable[[Any], Union[str, List[str]]]

@dataclass
class ColumnDefinition:
    datatype: str = ''
    hdx_datatype: Union[List[str], str] = ''
    identifier: str = ''
    nullable: bool = True
    ignored_field: bool = False
    ignored_reason: Optional[str] = None
    column_comes_from_object_field: bool = False

ReasonT = str


@dataclass
class DdlCreateTableInfo:
    project: Optional[str] = None
    tablename: Optional[str] = None
    columns: List[ColumnDefinition] = field(default_factory=list)
    candidate_primary_keys: Set[str] = field(default_factory=set)
    default_primary_key: Optional[str] = None
    final_primary_key: Optional[str] = None
    ignored_fields: List[ColumnDefinition] = field(default_factory=list)
    compression: str = 'none'
    ingest_type: str = 'csv'
    csv_delimiter: str = ','
    csv_input_indexes: Dict[str, int] = field(default_factory=dict)
    invalid_default_primary_key: Optional[str] = None


class NoDdlMappingFoundError(Exception):
    def __init__(self, sql_type):
        super().__init__(self)
        self.sql_type = sql_type


class IngestIndexError(Exception):
    pass


