from abc import ABC, abstractmethod
from typing import Iterator, List, Tuple, Union

from .common_intermediate_representation import ColumnDefinition, DdlCreateTableInfo


# pylint: disable=R0903
class SourceToTableInfoProcessor(ABC):
    @abstractmethod
    def yield_table_info_tokens(self, source_mapping: str,
                                create_tbl_info: DdlCreateTableInfo,
                                mapper) -> Iterator[Union[ColumnDefinition, Tuple[str, str]]]:
        """It returns an iterator that returns the name of the project and table to create
        as a tuple and, one by one, all columns that are encountered as the source_mapping
        is processed
        """


# pylint: disable=R0903
class ComposedTypeParser(ABC):
    @abstractmethod
    def parse(self, ddl_datatype, simple_datatypes_mapping,
              compound_datatypes_mapping) -> List[str]:
        pass
