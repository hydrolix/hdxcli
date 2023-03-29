from abc import ABC, abstractmethod
from typing import List, Callable, Tuple, Union
import json


from .common_intermediate_representation import NoDdlMappingFoundError


__all__ = ['ddl_to_hdx_datatype']


class ComposedTypeParser(ABC):
    @abstractmethod
    def parse(self, ddl_datatype, simple_datatypes_mapping, compound_datatypes_mapping) -> List[str]:
        pass


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


def ddl_to_hdx_datatype(data_mapping_file) -> Callable[[str], Union[str, List[str]]]:
    the_file = open(data_mapping_file, 'r')
    the_mapping = json.load(the_file)
    simple_datatypes_mapping = the_mapping['ddl_to_hdx_datatype']
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
            composed_parser = SqlComposedTypeParser()
            return composed_parser.parse(ddl_datatype, simple_datatypes_mapping, compound_datatypes)
        except KeyError as key_error:
            raise NoDdlMappingFoundError(ddl_datatype) from key_error
    return data_converter_func
