from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .common_intermediate_representation import (ColumnDefinition,
                                                 DdlCreateTableInfo,
                                                 DdlTypeToHdxTypeMappingFunc)


# pylint: disable=R0903
class SourceToTableInfoProcessor(ABC):
    @abstractmethod
    def yield_table_info_tokens(self, source_mapping: str,
                                mapper: DdlTypeToHdxTypeMappingFunc) -> Iterator[
                                    Union[ColumnDefinition,
                                          Tuple[str, str]]]:
        """Given a mapping in a ddl and a mapper, it returns an iterator that returns the
        name of the project and table to create a tuple and, one by one, all columns that
        are encountered as the source_mapping is processed.
        """


# pylint: disable=R0903
class ComposedTypeParser(ABC):
    @abstractmethod
    def parse(self, ddl_datatype,
              simple_datatypes_mapping,
              compound_datatypes_mapping) -> List[str]:
        pass


class PostProcessingHook(ABC):
    def post_process(self, ddl_create_table_info: DdlCreateTableInfo,
                     user_choices_dict: Dict[str, Any]):
        """
        This method receives all gathered create_table_info information during parsing
        and passes it through so that additional post-steps can be added.

        Some example of post-steps could be:

           - choosing the fields to index from a source in a csv after gathering the fields
           - select the type of csv separator if a csv file is expected for the transform

        You can implement your own ddl hook so that it will be executed when the main
        processing is done and DdlCreateTableInfo is filled.
        """
