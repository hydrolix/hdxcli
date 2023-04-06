from typing import Any, Callable, Dict, List, Optional, Union
import json

from ...library_api.common.interactive_helpers import (
    choose_interactively,
    choose_from_elements_interactively)

from .common_intermediate_representation import (NoDdlMappingFoundError,
                                                 ColumnDefinition,
                                                 DdlCreateTableInfo,
                                                 DdlTypeToHdxTypeMappingFunc)
from .interfaces import ComposedTypeParser, SourceToTableInfoProcessor, PostProcessingHook

# pylint: disable=wildcard-import
# pylint: disable=unused-wildcard-import
# All types from .extensions are used within this file from globals()['SomeType'] so
# the pylint and flake8 errors do not apply here for the strategy used to load
# types within this module. Alternatively importlib could be used instead of
# a top-level import but this makes clearer at the top-level the dependency.
from .extensions import *  # noqa: F403

from .exceptions import IngestIndexError, NoPrimaryKeyFoundException


__all__ = ['ddl_datatype_to_hdx_datatype', 'ddl_to_create_table_info', 'generate_transform_dict',
           'DdlTypeToHdxTypeMappingFunc']


def ddl_datatype_to_hdx_datatype(data_mapping_file, ddl_name: str) -> DdlTypeToHdxTypeMappingFunc:
    """
    Returns a function that translates a ddl composed type to a hdx composed type.
    The algorithm works by passing the ddl_name, which is used for custom
    compound type parsing and the data_mapping_file, which is also specific to each
    different ddl.
    """
    # pylint: disable=consider-using-with
    the_file = open(data_mapping_file, 'r', encoding='utf-8')
    the_mapping = json.load(the_file)
    simple_datatypes_mapping = the_mapping['simple_datatypes']
    compound_datatypes_mapping = the_mapping['compound_datatypes']
    the_file.close()

    def data_converter_func(ddl_datatype: Any) -> Union[str, List[str]]:
        dt = simple_datatypes_mapping.get(ddl_datatype)
        if isinstance(dt, list):
            for d in dt:
                if d.endswith('_optimal'):
                    return d.removesuffix('_optimal')
            else:
                raise RuntimeError(f'No optimal data type mapping found for {ddl_datatype}')
        if dt is not None:
            return dt
        try:
            composed_parser_type = globals()[ddl_name.lower().capitalize() + 'ComposedTypeParser']
            composed_parser: ComposedTypeParser = composed_parser_type()
            return composed_parser.parse(ddl_datatype,
                                         simple_datatypes_mapping,
                                         compound_datatypes_mapping)
        except KeyError as key_error:
            raise NoDdlMappingFoundError(ddl_datatype) from key_error
    return data_converter_func


def _select_csv_indexes(ddl: DdlCreateTableInfo):
    field_indexes = {}
    fields_available = sorted((c for c in ddl.columns), key=lambda c: c.identifier)
    field_index = -1
    ingest_index = -1

    indexes_used = set()
    done = False
    while not done:
        try:
            for i, a_field in enumerate(fields_available):
                print(f'{i + 1}. {a_field}')
            print()
            valid_choices = {str(x) for x in range(1, len(fields_available))}
            field_index = int(choose_interactively(
                'Please choose the field to assign an index for (Type Ctrl-C to finish): ',
                valid_choices=sorted(valid_choices)))
            field_internal_idx = field_index - 1
            the_field = fields_available[field_internal_idx]
            ingest_index = int(choose_interactively(
                f"Which index (starting at 0) do you want to assign for field {the_field}: ",
                valid_choices=list(set(range(len(valid_choices))) - indexes_used)))
            field_indexes[the_field.identifier] = ingest_index
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


def _quoted(string: str):
    return ''.join(["'", string, "'"])


def _select_potential_primary_key_candidates_maybe_interactive(
        create_table_info: DdlCreateTableInfo):
    cti = create_table_info
    candidates_str = ', '.join(_quoted(pc) for pc in cti.candidate_primary_keys)

    if not cti.candidate_primary_keys:
        raise NoPrimaryKeyFoundException('No primary keys to choose from.')

    if cti.default_primary_key and cti.candidate_primary_keys != 1:
        return choose_interactively(
            f"Please choose the primary key for your transform (default: '{cti.default_primary_key}'."
            f" candidates: {cti.candidate_primary_keys}): ",
            default=cti.default_primary_key, valid_choices=sorted(cti.candidate_primary_keys))
        return result
    elif cti.default_primary_key and cti.candidate_primary_keys == 1:
        if cti.default_primary_key != next(iter(cti.candidate_primary_keys)):
            return cti.default_primary_key
    elif not cti.default_primary_key and cti.candidate_primary_keys == 1:
        return next(iter(cti.candidate_primary_keys))
    elif not cti.default_primary_key and cti.candidate_primary_keys != 1:
        return choose_interactively(f'Please choose the primary key for your transform. '
                                    f'(candidates: {candidates_str}): ',
                                    valid_choices=sorted(cti.candidate_primary_keys))
    return None


def _select_compression(_: DdlCreateTableInfo):
    return choose_interactively(
        "Please choose compression type (suggestions: 'none', 'gzip'. Default is 'none'): ",
        default='none')


def _select_delimiter(_: DdlCreateTableInfo):
    return choose_interactively("Please choose the csv delimiter (default is ','): ",
                                default=',')


def _select_ingest_type(ddl: DdlCreateTableInfo):
    return choose_interactively(
        "Please choose type of transform (suggestions: 'csv', 'json'. Default is 'csv'): ",
        default='csv', valid_choices=['csv', 'json'])


def _select_ignored_fields_op(ddl: DdlCreateTableInfo):
    choice = choose_interactively(
        "Please choose whether ignored fields should be mapped "
        "as string columns or just ignored (default is false): ",
        default='false', valid_choices=['true', 'false'])
    return bool(choice.lower().capitalize())


ALL_USER_CHOICE_KEYS = ('primary_key',
                        'ingest_type',
                        'compression',
                        'csv_indexes',
                        'csv_delimiter',
                        'add_ignored_fields_as_string_columns')


USER_CHOICE_FUNCS: Dict[str, Callable[[DdlCreateTableInfo], Any]] = {
    'primary_key': _choose_primary_key,
    'compression': _select_compression,
    'csv_delimiter': _select_delimiter,
    'ingest_type': _select_ingest_type,
    'csv_indexes': _select_csv_indexes,
    'add_ignored_fields_as_string_columns': _select_ignored_fields_op}


def _try_apply_user_choice_from_dict(the_dict, key):
    if not the_dict or the_dict.get(key) is None:
        return (None, False)
    return (the_dict.get(key), True)


class GenericPostProcessingHook(PostProcessingHook):
    def post_process(self, ddl_create_table_info: DdlCreateTableInfo,
                     user_choices_dict: Dict[str, Any]):
        already_set_choices = {choice: False for choice in ALL_USER_CHOICE_KEYS}
        ignored_options_in_loop = {'csv_delimiter', 'csv_indexes'}
        ddl_table_fields_to_set = {'primary_key': 'final_primary_key'}
        if user_choices_dict:
            for user_choice in ALL_USER_CHOICE_KEYS:
                if user_choice in ignored_options_in_loop:
                    continue
                field_to_set = ddl_table_fields_to_set.get(user_choice, user_choice)
                field_val, already_set_choices[user_choice] = (
                    _try_apply_user_choice_from_dict(user_choices_dict, user_choice))
                if already_set_choices[user_choice]:
                    setattr(ddl_create_table_info, field_to_set, field_val)
            if ddl_create_table_info.ingest_type == 'csv':
                ddl_create_table_info.csv_delimiter = (
                    user_choices_dict.get('csv_delimiter') if
                    user_choices_dict.get('csv_delimiter') is not None
                    else USER_CHOICE_FUNCS['csv_delimiter'](ddl_create_table_info))
                already_set_choices['csv_delimiter'] = True

                if (indexes := user_choices_dict.get('csv_indexes')) is not None:
                    ddl_create_table_info.csv_input_indexes = {elem[0]: elem[1] for elem in indexes}
                    already_set_choices['csv_indexes'] = True
                else:
                    ddl_create_table_info.csv_input_indexes = USER_CHOICE_FUNCS['csv_indexes'](ddl_create_table_info)
                    already_set_choices['csv_indexes'] = True

        for user_choice, is_set_choice in already_set_choices.items():
            if not is_set_choice and user_choice not in ignored_options_in_loop:
                field_to_set = ddl_table_fields_to_set.get(user_choice, user_choice)
                setattr(ddl_create_table_info, field_to_set,
                        USER_CHOICE_FUNCS[user_choice](ddl_create_table_info))
                already_set_choices[user_choice] = True

        if ddl_create_table_info.ingest_type == 'csv':
            if not already_set_choices['csv_delimiter']:
                ddl_create_table_info.csv_delimiter = (
                    USER_CHOICE_FUNCS['csv_delimiter'](ddl_create_table_info))
            if not already_set_choices['csv_indexes']:
                ddl_create_table_info.csv_input_indexes = (
                    USER_CHOICE_FUNCS['csv_indexes'](ddl_create_table_info))


def ddl_to_create_table_info(source_mapping: str,
                             ddl_name: str,
                             ddl_to_hdx_mapping_func:
                             DdlTypeToHdxTypeMappingFunc, *,
                             user_choices_file: Optional[str]=None
                             ) -> DdlCreateTableInfo:
    """Given a source mapping (a create table in sql, for example, or an elastic json file),
        it fills a DdlCreateTableInfo with the necessary information to create a transform.
    """
    create_tbl_info = DdlCreateTableInfo()
    source_to_ti_proc: SourceToTableInfoProcessor = (
        globals()[f'{ddl_name.lower().capitalize()}SourceToTableInfoProcessor']())

    for column_or_table_and_project_name in source_to_ti_proc.yield_table_info_tokens(
            source_mapping, ddl_to_hdx_mapping_func):
        if isinstance(column_or_table_and_project_name, ColumnDefinition):
            if (column_or_table_and_project_name.hdx_datatype in ('datetime', 'epoch') and
                    not column_or_table_and_project_name.column_comes_from_object_field):
                create_tbl_info.default_primary_key = column_or_table_and_project_name.identifier
                create_tbl_info.candidate_primary_keys.add(
                    column_or_table_and_project_name.identifier)

            if not column_or_table_and_project_name.ignored_field:
                create_tbl_info.columns.append(column_or_table_and_project_name)
            else:
                create_tbl_info.ignored_fields.append(column_or_table_and_project_name)
        elif isinstance(column_or_table_and_project_name, tuple):
            create_tbl_info.project, create_tbl_info.tablename = column_or_table_and_project_name
        else:
            assert False

    user_choices_dict = {}
    if user_choices_file:
        with open(user_choices_file,
                  encoding='utf-8') as user_choices_stream:
            user_choices_dict = json.load(user_choices_stream)
    GenericPostProcessingHook().post_process(create_tbl_info, user_choices_dict)
    try:

        globals()[ddl_name.lower().capitalize() +
                  'PostProcessingHook']().post_process(create_tbl_info,
                                                       user_choices_dict.get(ddl_name.lower()))
    except KeyError:
        pass
    return create_tbl_info


def _is_composed_datatype(col_def: ColumnDefinition):
    return isinstance(col_def.hdx_datatype, list)


def _needs_index(col_def: ColumnDefinition,
                 ddl: DdlCreateTableInfo):
    if _is_composed_datatype(col_def):
        return False
    return col_def.hdx_datatype != 'double' and ddl.final_primary_key != col_def.identifier


def _create_transform_elements_for(col_def: ColumnDefinition):
    if not _is_composed_datatype(col_def):
        return []
    hdx_datatype_raw = col_def.hdx_datatype
    result = []
    for typename in hdx_datatype_raw[1:]:
        dict_result = {"type": typename,
                       "index": typename != 'double',
                       "denullify": True}
        the_default: Optional[Any] = None
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


def _create_transform_output_column(col_def: ColumnDefinition,
                                    ddl: DdlCreateTableInfo,
                                    *,
                                    from_input_index=None,
                                    is_primary_key=False):
    output_column: Dict[str, Any] = {}
    output_column['name'] = col_def.identifier
    output_column['datatype'] = {}
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
    the_transform: Dict[str, Any] = {}
    the_transform['name'] = transform_name
    the_transform['settings'] = {}
    the_transform['description'] = description
    the_transform['type'] = transform_type
    # the_transform['format_details'] = {}
    # the_transform['format_details']["delimiter"] = ddl.csv_delimiter

    the_transform['settings']['is_default'] = False
    the_transform['settings']['output_columns'] = []
    the_transform['settings']['compression'] = ddl.compression
    output_columns: List[ColumnDefinition] = the_transform['settings']['output_columns']
    for col in ddl.columns:

        output_columns.append(
            _create_transform_output_column(col,
                                            ddl,
                                            from_input_index=(
                                                ddl.csv_input_indexes.get(col.identifier)),
                                            is_primary_key=(
                                                ddl.final_primary_key == col.identifier)))
    return the_transform
