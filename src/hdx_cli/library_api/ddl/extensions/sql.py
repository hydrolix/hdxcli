from enum import Enum
from typing import Iterator, List, Union, Tuple

import sqlglot

from ..common_intermediate_representation import (ColumnDefinition,
                                                  DdlCreateTableInfo,
                                                  DdlTypeToHdxTypeMappingFunc)
from ..interfaces import SourceToTableInfoProcessor, ComposedTypeParser


__all__ = ['SqlSourceToTableInfoProcessor', 'SqlComposedTypeParser']


# pylint: disable=R0903
class SqlComposedTypeParser(ComposedTypeParser):
    def parse(self, ddl_datatype: str,
              simple_datatypes_mapping,
              compound_datatypes_mapping) -> List[str]:
        if ddl_datatype.startswith('STRUCT'):
            return [compound_datatypes_mapping['STRUCT'],
                    'string', 'string']
        inner_type = None
        outer_type = None
        if ddl_datatype.startswith('ARRAY'):
            outer_type = 'ARRAY'
            data_type_no_spaces = ddl_datatype.replace(' ', '')
            marker_start_inner = data_type_no_spaces.index('<')
            outer_type = data_type_no_spaces[:marker_start_inner]
            inner_type = data_type_no_spaces[marker_start_inner + 1:ddl_datatype.index('>')]
        else:
            inner_type = 'TEXT'
            outer_type = 'ARRAY'
        the_inner_type: str = simple_datatypes_mapping[inner_type]

        if isinstance(the_inner_type, list):
            the_inner_type = [t for t in the_inner_type
                              if t.endswith("_optimal")][0].removesuffix("_optimal")
        return [compound_datatypes_mapping[outer_type], the_inner_type]


def normalize_variable_name(variable_with_hyphens):
    return variable_with_hyphens.replace('-', '_')


def find_next_backtick_pair(text, initial_offset=0):
    first_backtick = text.find('`', initial_offset)
    second_backtick = None
    if first_backtick != -1:
        second_backtick = first_backtick + 1 + text[first_backtick + 1:].find('`') + 1
    else:
        first_backtick = None
    return first_backtick, second_backtick


def replace_query_backtick_vars(query_create_table):
    open_backtick, close_backtick = find_next_backtick_pair(query_create_table)
    query_create_table_modified = query_create_table
    original_vars = []
    while open_backtick is not None:
        original_var = query_create_table_modified[open_backtick:close_backtick]
        original_vars.append(original_var)
        query_create_table_modified = query_create_table_modified.replace(
            query_create_table_modified[open_backtick:close_backtick],
            normalize_variable_name(
                query_create_table_modified
                [open_backtick + 1: close_backtick - 1]))
        open_backtick, close_backtick = find_next_backtick_pair(
            query_create_table_modified, close_backtick)
    return (query_create_table_modified, original_vars)


class ConstraintKind(Enum):
    NULLABLE = 0
    PRIMARY_KEY = 1
    # pass


class ParseContext(Enum):
    UNKNOWN = 0
    TABLE = 1
    COLUMN = 2
    COLUMNCONSTRAINT = 3


class NoDdlMappingFoundError(Exception):
    def __init__(self, ddl_type):
        super().__init__(self)
        self.sql_type = ddl_type


# pylint: disable=R0903
class SqlSourceToTableInfoProcessor(SourceToTableInfoProcessor):
    def yield_table_info_tokens(self, source_mapping: str,
                                mapper: DdlTypeToHdxTypeMappingFunc) -> Iterator[Union[ColumnDefinition, Tuple[str, str]]]:
        modified_source_mapping, _ = replace_query_backtick_vars(source_mapping)
        sqlglot_create_expression = sqlglot.parse_one(modified_source_mapping)
        for node in sqlglot_create_expression.bfs():
            if isinstance(node[0], sqlglot.expressions.Table):
                project, tablename = [str(n) for n
                                      in list(node[0].flatten())]
                yield (project, tablename)
            if isinstance(node[0], sqlglot.expressions.ColumnDef):
                identifier_datatype_maybeconstraint = list(node[0].flatten())
                identifier, datatype = (identifier_datatype_maybeconstraint[0],
                                        identifier_datatype_maybeconstraint[1])
                constraint = None
                try:
                    constraint = identifier_datatype_maybeconstraint[2]
                except IndexError:
                    pass
                is_nullable = True
                if constraint and isinstance(constraint, sqlglot.expressions.ColumnConstraint):
                    # pylint: disable=stop-iteration-return
                    # activated here because next never raises StopIteration in this branch
                    constraint_type = next(constraint.flatten())
                    if isinstance(constraint_type, sqlglot.expressions.NotNullColumnConstraint):
                        is_nullable = False
                    elif isinstance(constraint_type,
                                    sqlglot.expressions.PrimaryKeyColumnConstraint):
                        is_nullable = False
                yield ColumnDefinition(datatype=datatype.sql(),
                                       hdx_datatype=mapper(datatype.sql()),
                                       identifier=identifier.name,
                                       nullable=is_nullable)
