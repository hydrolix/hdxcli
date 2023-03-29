from abc import ABC, abstractmethod
from typing import Any, List, Optional

from .common_intermediate_representation import ColumnDefinition


# pylint: disable=R0903
class ColumnDefinitionsProvider(ABC):
    @abstractmethod
    def yield_column(self, column_input: Any) -> ColumnDefinition:
        pass


# pylint: disable=R0903
class ComposedTypeParser(ABC):
    @abstractmethod
    def parse(self, ddl_datatype, simple_datatypes_mapping, compound_datatypes_mapping) -> List[str]:
        pass
