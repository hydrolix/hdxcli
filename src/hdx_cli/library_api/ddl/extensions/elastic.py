from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Union, Tuple

import json

from ....library_api.common.interactive_helpers import (
    choose_interactively,
    choose_from_elements_interactively)

from ..common_intermediate_representation import ColumnDefinition, DdlCreateTableInfo
from ..interfaces import (ComposedTypeParser, SourceToTableInfoProcessor,
                          PostProcessingHook)

from ...common.exceptions import NotSupportedException

__all__ = ['ElasticSourceToTableInfoProcessor',
           'ElasticComposedTypeParser']


# pylint: disable=R0903
class ElasticComposedTypeParser(ComposedTypeParser):
    def parse(self, ddl_datatype,
              simple_datatypes_mapping,
              compound_datatypes_mapping) -> List[str]:
        pass


class ConstraintKind(Enum):
    NULLABLE = 0
    PRIMARY_KEY = 1
    # pass


class ParseContext(Enum):
    UNKNOWN = 0
    TABLE = 1
    COLUMN = 2
    COLUMNCONSTRAINT = 3

Name = str
Identifier = str


def _do_parse_elastic_object_type(base_name, value, results):
    if the_type := value.get('type'):
        results.append([base_name, the_type])
    else:
        for prop_name, prop_value in value['properties'].items():
            _do_parse_elastic_object_type(base_name + '__' + prop_name,
                                          prop_value, results)


def _parse_elastic_object_type(base_name, value, mapper):
    results = []
    _do_parse_elastic_object_type(base_name, value, results)
    for field_name, datatype in results:
        if (mapped_datatype := mapper(datatype)) is not None:
            yield ColumnDefinition(datatype=field_name,
                                   hdx_datatype=mapped_datatype,
                                   identifier=field_name,
                                   nullable=True,
                                   column_comes_from_object_field=True)
        else:
            yield ColumnDefinition(datatype=datatype,
                                   identifier=field_name,
                                   ignored_field=True)


# pylint: disable=R0903
class ElasticSourceToTableInfoProcessor(SourceToTableInfoProcessor):
    def yield_table_info_tokens(self, source_mapping,
                                mapper) -> Iterator[Union[ColumnDefinition, Tuple[str, str]]]:
        elastic_json = json.loads(source_mapping)
        top_level_key = list(elastic_json.keys())[0]
        elastic_mappings = elastic_json[top_level_key]['mappings']
        if elastic_mappings['dynamic'].lower() in ('true'):
            raise NotSupportedException('Elastic dynamic mappings are not supported')
        prj_name, tbl_name = top_level_key, top_level_key
        yield (prj_name, tbl_name)
        for prop_name, prop_value in elastic_mappings["properties"].items():
            if prop_type := prop_value.get('type'):
                if prop_type == 'alias':
                    yield ColumnDefinition(datatype=prop_type,
                                           identifier=prop_name,
                                           ignored_field=True)
                else:
                    yield ColumnDefinition(datatype=prop_type,
                                           hdx_datatype=mapper(prop_type),
                                           identifier=prop_name,
                                           nullable=True)
            # objects
            else:
                yield from _parse_elastic_object_type(prop_name, prop_value, mapper)


def _select_array_columns(fields):
    fields_chosen = set()
    while True:
        try:
            idx, field = choose_from_elements_interactively(fields)
            fields_chosen.add(field)
            fields.pop(idx)
        except KeyboardInterrupt:
            print()
            break
    return fields_chosen


class ElasticPostProcessingHook(PostProcessingHook):
    all_user_choices = {'array_fields'}
    all_user_choices_funcs = {'array_fields', _select_array_columns}

    def post_process(self, ddl_create_table_info: DdlCreateTableInfo,
                     user_choices_dict: Dict[str, Any]):
        # Used choices contains which options have been set so far,
        used_choices = {choice: False for choice in type(self).all_user_choices}
        if user_choices_dict and (array_fields := user_choices_dict.get('array_fields')):
            for field in array_fields:
                try:
                    idx = ddl_create_table_info.columns.index(field)
                    col_data_type = ddl_create_table_info.columns[idx].hdx_datatype
                    assert isinstance(col_data_type, str)
                    col_data_type = ['array', col_data_type]
                except ValueError:
                    print(f'WARNING: ignored inexistent field {field} in elastic.array_fields')
            used_choices['array_fields'] = True
        if not used_choices.get('array_fields'):
            the_cols = set({c for c in ddl_create_table_info.columns
                            if not c.column_comes_from_object_field})
            sorted_columns = sorted(the_cols, key=lambda c: c.identifier)
            print('Press Ctrl-C when you are done.')
            chosen_columns = _select_array_columns(sorted_columns)
            for col in chosen_columns:
                col.hdx_datatype = ['array', col.hdx_datatype]
