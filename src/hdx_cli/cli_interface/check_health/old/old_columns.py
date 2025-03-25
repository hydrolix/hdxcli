# type: ignore
import logging

from . import const


class DataType:
    def __init__(
        self,
        type: str | None = None,
        primary: bool | None = None,
        resolution: str | None = None,
        index: bool | None = None,
        elements: list | None = None,
        ignore: bool | None = None,
        suppress: bool | None = None,
        **kwargs,
    ):
        self.type = type
        self.primary = primary or False
        self.raw_resolution = resolution
        self.index = index or self.is_indexable
        self.elements = elements or []
        self.suppress = suppress
        self.ignore = ignore
        self.kwargs = kwargs

    @property
    def skip(self):
        """should this column be skipped?"""
        return self.suppress or self.ignore

    @property
    def is_indexable(self):
       

    @property
    def can_be_primary(self):
        """Can this column be primary?"""
        return self.type in const.DATETIME_TYPES

    @property
    def correct_type(self):
        """Return the corrected type of this column"""
        if self.type in const.DATETIME_TYPES:
            if self.resolution == const.RESOLUTION_MILLISECOND:
                return const.TYPE_DATETIME64
            if self.resolution == const.RESOLUTION_SECOND:
                return const.TYPE_DATETIME
        return self.type

    @property
    def correct_primary(self) -> bool:
        """return primary if this datatype can be primary"""
        if not self.can_be_primary:
            return False
        return self.primary

    @property
    def correct_index(self) -> bool:
        """return index if this datatype can be indexed"""
        if not self.is_indexable:
            return False
        return self.index

    @property
    def resolution(self) -> str | None:
        """Get the normalized resolution"""
        if not self.raw_resolution:
            return None
        return const.RESOLUTION_MAP.get(self.raw_resolution, None)

    @property
    def element_types(self) -> set:
        """Get all the types of elements"""
        retval = set()
        if self.type not in const.COMPLEX_TYPES:
            return retval
        if not self.elements:
            return retval
        for element in self.elements:
            element_type = element.get("type", None)
            if element_type:
                retval.add(element_type)
        return set(retval)

    @property
    def corrected_datatype(self) -> dict:
        """return a corrected version of this datatype if possible"""
        # Unimplemented
        return {}

    # Report Checks
    def _primary_report(self, report, name):
        if self.primary:
            if not self.can_be_primary:
                report.add_message(
                    logging.ERROR,
                    f"Column '{name}' primary enabled, but can’t be primary",
                )
        return report

    def _type_report(self, report, name):
        if self.type not in const.ALL_TYPES:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' type is '{self.type}', which is not a valid type",
            )

        if self.type != self.correct_type:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' type is {self.type}, should be {self.correct_type}",
            )
        return report

    def _resolution_report(self, report, name):
        if self.type not in const.DATETIME_TYPES:
            return report
        if not self.resolution:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' type is '{self.type}', resolution is required, but not set",
            )
        return report

    def _elements_report(self, report, name):
        if self.type not in const.COMPLEX_TYPES:
            return report
        correct_element_count = const.COMPLEX_ELEMENT_COUNTS.get(self.type)
        if len(self.elements) != correct_element_count:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' has {self.elements.count} elements, should have {correct_element_count}",
            )
        return report

    def report(self, report, name):
        """Report on this column"""
        report_checks = [
            self._index_report,
            self._type_report,
            self._resolution_report,
            self._primary_report,
            self._elements_report,
        ]
        for check in report_checks:
            report = check(report, name)
        return report


class Column:
    """A single column with a datatype to be checked"""

    def __init__(self, name: str | None = "", datatype: dict | None = {}):
        self.name = name or ""
        self.raw_datatype = datatype or {}
        self.datatype = DataType(**self.raw_datatype)

    # Datatype pass-through properties
    @property
    def skip(self):
        return self.datatype.skip

    @property
    def primary(self):
        """Is this a primary column"""
        return self.datatype.primary and self.datatype.can_be_primary

    def report(self, report):
        """Report on this column"""
        name = self.name

        if not name:
            report.add_message(logging.ERROR, "Column has no name")
        report = self.datatype.report(report, name)
        return report
