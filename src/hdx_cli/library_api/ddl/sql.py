from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Set, Union, Dict, Any, Tuple, Sequence

import json
import sqlglot

from .common_intermediate_representation import ColumnDefinition
from .common_algo import ddl_to_hdx_datatype
from .user_ddl_customization_interfaces import ColumnDefinitionsProvider


__all__ = ['ddl_to_create_table_info',
           'DdlCreateTableInfo',
           'ColumnDefinition',
           'generate_transform_dict']


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
        query_create_table_modified = query_create_table_modified.replace(query_create_table_modified
                [open_backtick:close_backtick],
                normalize_variable_name(
                    query_create_table_modified
                    [open_backtick + 1: close_backtick - 1]))
        open_backtick, close_backtick = find_next_backtick_pair(query_create_table_modified, close_backtick)
    return (query_create_table_modified, original_vars)


@dataclass
class DdlCreateTableInfo:
    project: Optional[str] = None
    tablename: Optional[str] = None
    columns: List[ColumnDefinition] = field(default_factory=list)
    candidate_primary_keys: Set[str] = field(default_factory=set)
    default_primary_key: Optional[str] = None
    final_primary_key: Optional[str] = None
    ignored_fields: List[str] = field(default_factory=list)
    compression: str = 'none'
    ingest_type: str = 'csv'
    csv_delimiter: str = ','
    csv_input_indexes: Dict[str, int] = field(default_factory=dict)
    invalid_default_primary_key: Optional[str] = None


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
class SqlColumnDefinitionsProvider(ColumnDefinitionsProvider):
    def yield_column(self, column_input: Any) -> ColumnDefinition:
        node = column_input[0]
        create_tbl_info = column_input[1]
        mapper = column_input[2]
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
                constraint_type = next(constraint.flatten())
                if isinstance(constraint_type, sqlglot.expressions.NotNullColumnConstraint):
                    is_nullable = False
                elif isinstance(constraint_type, sqlglot.expressions.PrimaryKeyColumnConstraint):
                    is_nullable = False
                    if datatype.sql() == 'TIMESTAMP':
                        create_tbl_info.default_primary_key = identifier.name
                        create_tbl_info.candidate_primary_keys.add(identifier.name)
                    else:
                        create_tbl_info.invalid_default_primary_key = identifier.name
            elif datatype.sql() == 'TIMESTAMP':
                create_tbl_info.candidate_primary_keys.add(identifier.name)

            return ColumnDefinition(datatype=datatype.sql(),
                                    hdx_datatype=mapper(datatype.sql()),
                                    identifier=identifier.name,
                                    nullable=is_nullable)


def ddl_to_create_table_info(source_mapping: str,
                             mapper) -> DdlCreateTableInfo:
    """Given a source mapping (a create table in sql, for example, or an elastic json file),
        it fills a DdlCreateTableInfo with the necessary information to create a transform.
    """
    modified_source_mapping, _ = replace_query_backtick_vars(source_mapping)
    sqlglot_create_expression = sqlglot.parse_one(modified_source_mapping)
    create_tbl_info = DdlCreateTableInfo()
    columns_provider = SqlColumnDefinitionsProvider()
    for node in sqlglot_create_expression.bfs():
        if isinstance(node[0], sqlglot.expressions.Table):
            create_tbl_info.project, create_tbl_info.tablename = [str(n) for n
                                                                  in list(node[0].flatten())]
        if isinstance(node[0], sqlglot.expressions.ColumnDef):
            new_column = columns_provider.yield_column((node, create_tbl_info, mapper))
            create_tbl_info.columns.append(new_column)


    create_tbl_info.final_primary_key = _choose_primary_key(create_tbl_info)
    create_tbl_info.compression = _select_compression()
    (create_tbl_info.ingest_type,
     create_tbl_info.csv_delimiter,
     create_tbl_info.csv_input_indexes) = _select_transform_type(create_tbl_info)

    return create_tbl_info


def _create_transform_elements_for(col_def: ColumnDefinition):
    if not _is_composed_datatype(col_def):
        return []
    hdx_datatype_raw = col_def.hdx_datatype
    result = []
    for typename in hdx_datatype_raw[1:]:
        dict_result = {"type": typename,
                       "index": typename != 'double',
                       "denullify": True}
        the_default = None
        if (typename.startswith('int') or typename.startswith('uint') or
                typename == 'double'):
            the_default = 0
        elif typename == 'string':
            the_default = ''
        if the_default:
            dict_result = {"default": the_default}

        if typename == 'datetime':
            dict_result['format'] = "2006-01-02T15:04:05.999999999Z"
            dict_result['resolution'] = 'ns'
        if typename == 'epoch':
            dict_result['format'] = 'us'
            dict_result['resolution'] = 's'
        result.append(dict_result)
    return result


# This can only be used for inner type of composed types
def _create_inner_type_dict(typename: Union[str, Tuple[str]]):
    if isinstance(typename, tuple):
        raise RuntimeError(f'{typename}')

    dict_result = {"type": typename,
                   "index": typename != 'double',
                   "denullify": True}
    the_default = None
    if (typename.startswith('int') or typename.startswith('uint') or
            typename == 'double'):
        the_default = 0
    elif typename == 'string':
        the_default = ''
    if the_default:
        dict_result = {"default": the_default}

    if typename == 'datetime':
        dict_result['format'] = "2006-01-02T15:04:05.999999999Z"
        dict_result['resolution'] = 'ns'
    if typename == 'epoch':
        dict_result['format'] = 'us'
        dict_result['resolution'] = 's'
    return [dict_result]


def _is_composed_datatype(col_def: ColumnDefinition):
    return isinstance(col_def.hdx_datatype, list)


def _needs_index(col_def: ColumnDefinition,
                 ddl: DdlCreateTableInfo):
    if _is_composed_datatype(col_def):
        return False
    return col_def.hdx_datatype != 'double' and ddl.final_primary_key != col_def.identifier


def _create_transform_output_column(col_def: ColumnDefinition,
                                    ddl: DdlCreateTableInfo,
                                    *,
                                    from_input_index=None,
                                    is_primary_key=False):
    output_column = {}
    output_column['name'] = col_def.identifier
    output_column['datatype'] : Dict[str, Any] = {}

    hdx_datatype_raw = col_def.hdx_datatype

    datatype = output_column['datatype']
    datatype['type'] = hdx_datatype_raw if not isinstance(hdx_datatype_raw, list) else hdx_datatype_raw[0]
    datatype['index'] = _needs_index(col_def, ddl)
    if col_def.hdx_datatype == 'datetime':
        datatype['format'] = "2006-01-02 15:04:05.000000Z"
    datatype['ignore'] = False
    datatype['script'] = None
    datatype['source'] = None
    datatype['default'] = None
    datatype['primary'] = is_primary_key
    datatype['virtual'] = False

    if from_input_index is not None:
        datatype['source'] = {'from_input_index': from_input_index}

    datatype['elements'] = _create_transform_elements_for(col_def)

    datatype['catch_all'] = False
    datatype['resolution'] = "seconds"

    return output_column


def generate_transform_dict(ddl: DdlCreateTableInfo,
                            transform_name: str,
                            *,
                            description: str = None,
                            transform_type: str = 'csv'):
    the_transform = {}
    the_transform['name'] = transform_name
    the_transform['settings'] = {}
    the_transform['description'] = description
    the_transform['type'] = transform_type
#   the_transform['format_details']["delimiter"] = ddl.csv_delimiter

    the_transform['settings']['is_default'] = False
    the_transform['settings']['output_columns']: List[ColumnDefinition] = []
    the_transform['settings']['compression'] = ddl.compression
    output_columns: List[ColumnDefinition] = the_transform['settings']['output_columns']

    col: ColumnDefinition
    for col in ddl.columns:
        output_columns.append(
            _create_transform_output_column(col,
                                            ddl,
                                            from_input_index=ddl.csv_input_indexes.get(col.identifier),
                                            is_primary_key=(ddl.final_primary_key == col.identifier)))
    return the_transform


def _quoted(string: str):
    return ''.join(["'", string, "'"])


def _select_potential_primary_key_candidates_maybe_interactive(create_table_info: DdlCreateTableInfo):
    cti = create_table_info
    candidates = ', '.join(_quoted(pc) for pc in cti.candidate_primary_keys)
    if cti.default_primary_key and cti.candidate_primary_keys != 1:
        result = input(f"Please choose the primary key for your transform (default: '{cti.default_primary_key}'. "
                       f'candidates: {candidates}): ')
        if result in cti.candidate_primary_keys:
            return result
        return cti.default_primary_key
    elif cti.default_primary_key and cti.candidate_primary_keys == 1:
        if cti.default_primary_key != next(iter(cti.candidate_primary_keys)):
            return cti.default_primary_key
    elif not cti.default_primary_key and cti.candidate_primary_keys == 1:
        return next(iter(cti.candidate_primary_keys))
    elif not cti.default_primary_key and cti.candidate_primary_keys != 1:
        result = input(f'Please choose the primary key for your transform. '
                       f'(candidates: {candidates}): ')
        if result not in cti.candidate_primary_keys:
            return None
        return result
    return None


def _select_compression():
    compression = input(f"Please choose compression type (suggestions: 'none', 'gzip'. Default is 'none'): ")
    if compression == '':
        return 'none'
    return compression


def _select_transform_type(ddl: DdlCreateTableInfo):
    ttype = input(f"Please choose type of transform (suggestions: 'csv', 'json'. Default is 'csv'): ")
    if ttype == '':
        ttype = 'csv'

    delimiter = ''
    fields_indexes = {}
    if ttype == 'csv':
        delimiter = input(f"Please choose the csv delimiter (default is ','): ")
        fields_indexes = _select_csv_indexes(ddl)
    if delimiter == '' and ttype == 'csv':
        delimiter = ','

    return (ttype, delimiter, fields_indexes)


class IngestIndexError(Exception):
    pass


def _select_csv_indexes(ddl: DdlCreateTableInfo):
    field_indexes = {}
    fields_available = sorted([c.identifier for c in ddl.columns])
    field_index = -1
    ingest_index = -1

    indexes_used = set()
    done = False
    while not done:
        try:
            for i, a_field in enumerate(fields_available):
                print(f'{i + 1}. {a_field}')
            print()
            field_index = int(input(
                'Please choose the field to assign an index for (Type Ctrl-C to finish): '))
            field_internal_idx = field_index - 1
            the_field = fields_available[field_internal_idx]
            ingest_index = int(input(
                "Which index do you want to assign for field (Type Ctrl-C to to finish)" +
                f" '{the_field}': "))
            if ingest_index in indexes_used:
                raise IngestIndexError(f'Index already used {ingest_index}. Use another index.')

            field_indexes[the_field] = ingest_index
            fields_available.remove(the_field)
            indexes_used.add(ingest_index)
        except KeyboardInterrupt:
            done = True
            print()
        except IndexError:
            pass
        except ValueError:
            pass
        except IngestIndexError as ingest_err:
            print(f'Error: {ingest_err}')
            input('Press Enter to continue')
            print()
    return field_indexes


def _try_select_potential_primary_key(cti: DdlCreateTableInfo):
    return _select_potential_primary_key_candidates_maybe_interactive(cti)


def _choose_primary_key(cti: DdlCreateTableInfo):
    not_done = True
    p_key = None
    while not_done:
        p_key = _try_select_potential_primary_key(cti)
        if p_key:
            not_done = False
        else:
            print()
    return p_key


if __name__ == '__main__':
    import sys

    a_query_create_table: Optional[str] = None
    with open(sys.argv[1], encoding='utf-8') as f:
        a_query_create_table = f.read()
    a_mapper = ddl_to_hdx_datatype(sys.argv[2])
    create_table_info: DdlCreateTableInfo = ddl_to_create_table_info(a_query_create_table, a_mapper)
    print('Candidate primary keys:', create_table_info.candidate_primary_keys)
    print('Default primary key:', create_table_info.default_primary_key)
    print('Table name: ', create_table_info.tablename)
    print('Invalid default primary key: ', create_table_info.invalid_default_primary_key)
