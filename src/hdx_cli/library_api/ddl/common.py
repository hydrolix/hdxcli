from dataclasses import dataclass, field
from enum import Enum
from typing import List, Union

from .common_intermediate_representation import (ColumnDefinition,
                                                 DdlCreateTableInfo)


__all__ = ['ddl_to_create_table_info',
           'ddl_to_hdx_datatype',
           'needs_index',
           'generate_transform_dict']


def ddl_to_hdx_datatype(data_mapping_file) -> Union[str, List[str]]:
    pass


def ddl_to_create_table_info(elastic_mapping: str,
                             mapper) -> DdlCreateTableInfo:
    pass


def _create_transform_elements_for(col_def: ColumnDefinition):
    pass


def needs_index(col_def: ColumnDefinition,
                ddl: DdlCreateTableInfo):
    pass


def _create_transform_output_column(col_def: ColumnDefinition,
                                    ddl: DdlCreateTableInfo,
                                    *,
                                    from_input_index=None,
                                    is_primary_key=False):
    pass


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


# def _quoted(string: str):
#     return ''.join(["'", string, "'"])


# def _select_potential_primary_key_candidates_maybe_interactive(create_table_info: DdlCreateTableInfo):
#     cti = create_table_info
#     candidates = ', '.join(_quoted(pc) for pc in cti.candidate_primary_keys)
#     if cti.default_primary_key and cti.candidate_primary_keys != 1:
#         result = input(f"Please choose the primary key for your transform (default: '{cti.default_primary_key}'. "
#                        f'candidates: {candidates}): ')
#         if result in cti.candidate_primary_keys:
#             return result
#         return cti.default_primary_key
#     elif cti.default_primary_key and cti.candidate_primary_keys == 1:
#         if cti.default_primary_key != next(iter(cti.candidate_primary_keys)):
#             return cti.default_primary_key
#     elif not cti.default_primary_key and cti.candidate_primary_keys == 1:
#         return next(iter(cti.candidate_primary_keys))
#     elif not cti.default_primary_key and cti.candidate_primary_keys != 1:
#         result = input(f'Please choose the primary key for your transform. '
#                        f'(candidates: {candidates}): ')
#         if result not in cti.candidate_primary_keys:
#             return None
#         return result
#     return None


# def _select_compression():
#     compression = input(f"Please choose compression type (suggestions: 'none', 'gzip'. Default is 'none'): ")
#     if compression == '':
#         return 'none'
#     return compression


# def _select_transform_type(ddl: DdlCreateTableInfo):
#     ttype = input(f"Please choose type of transform (suggestions: 'csv', 'json'. Default is 'csv'): ")
#     if ttype == '':
#         ttype = 'csv'

#     delimiter = ''
#     fields_indexes = {}
#     if ttype == 'csv':
#         delimiter = input(f"Please choose the csv delimiter (default is ','): ")
#         fields_indexes = _select_csv_indexes(ddl)
#     if delimiter == '' and ttype == 'csv':
#         delimiter = ','

#     return (ttype, delimiter, fields_indexes)


# def _select_csv_indexes(ddl: DdlCreateTableInfo):
#     field_indexes = {}
#     fields_available = sorted([c.identifier for c in ddl.columns])
#     field_index = -1
#     ingest_index = -1

#     indexes_used = set()
#     done = False
#     while not done:
#         try:
#             for i, a_field in enumerate(fields_available):
#                 print(f'{i + 1}. {a_field}')
#             print()
#             field_index = int(input(
#                 'Please choose the field to assign an index for (Type Ctrl-C to finish): '))
#             field_internal_idx = field_index - 1
#             the_field = fields_available[field_internal_idx]
#             ingest_index = int(input(
#                 f"Which index do you want to assign for field (Type Ctrl-C to to finish) '{the_field}': "))
#             if ingest_index in indexes_used:
#                 raise IngestIndexError(f'Index already used {ingest_index}. Use another index.')

#             field_indexes[the_field] = ingest_index
#             fields_available.remove(the_field)
#             indexes_used.add(ingest_index)
#         except KeyboardInterrupt:
#             done = True
#             print()
#         except IndexError:
#             pass
#         except ValueError:
#             pass
#         except IngestIndexError as ingest_err:
#             print(f'Error: {ingest_err}')
#             input('Press Enter to continue')
#             print()
#     return field_indexes


# def _try_select_potential_primary_key(cti: DdlCreateTableInfo):
#     return _select_potential_primary_key_candidates_maybe_interactive(cti)


# def _choose_primary_key(cti: DdlCreateTableInfo):
#     not_done = True
#     p_key = None
#     while not_done:
#         p_key = _try_select_potential_primary_key(cti)
#         if p_key:
#             not_done = False
#         else:
#             print()
#     return p_key
