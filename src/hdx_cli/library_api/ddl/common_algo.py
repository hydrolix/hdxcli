from abc import ABC, abstractmethod
from typing import List, Callable, Union
import json

from .common_intermediate_representation import (NoDdlMappingFoundError,
                                                 DdlCreateTableInfo)
from .user_ddl_customization_interfaces import ComposedTypeParser

__all__ = ['ddl_to_hdx_datatype']


# pylint: disable=R0903
class SqlComposedTypeParser(ComposedTypeParser):
    def parse(self, ddl_datatype,
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


# pylint: disable=R0903
class ElasticComposedTypeParser(ComposedTypeParser):
    def parse(self, ddl_datatype,
              simple_datatypes_mapping,
              compound_datatypes_mapping) -> List[str]:
        pass


def ddl_to_hdx_datatype(data_mapping_file, ddl_name: str) -> Callable[[str], Union[str, List[str]]]:
    """
    Returns a function that translates a ddl type to a hdx type.
    The algorithm works by passing the ddl_name, which is used for custom
    compound type parsing and the data_mapping_file, which is also specific to each
    different ddl. Namely, for each ddl there are two customization points used
    inside the algorithm.
    """
    # pylint: disable=consider-using-with
    the_file = open(data_mapping_file, 'r', encoding='utf-8')
    the_mapping = json.load(the_file)
    simple_datatypes_mapping = the_mapping['simple_datatypes']
    compound_datatypes = the_mapping['compound_datatypes']
    the_file.close()

    def data_converter_func(ddl_datatype: str) -> Union[str, List[str]]:
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
            composed_parser = composed_parser_type()
            return composed_parser.parse(ddl_datatype, simple_datatypes_mapping, compound_datatypes)
        except KeyError as key_error:
            raise NoDdlMappingFoundError(ddl_datatype) from key_error
    return data_converter_func


def ddl_to_create_table_info(source_mapping: str,
                             ddl_to_hdx_type_mapper) -> DdlCreateTableInfo:
    pass
