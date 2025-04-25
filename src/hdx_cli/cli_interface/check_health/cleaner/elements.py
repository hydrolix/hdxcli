from __future__ import annotations

import logging

from .. import const, reportlog


class Element:
    """A single element from a complex column"""

    def __init__(self, element: dict) -> None:
        self.raw_element = element
        self.element_type = element.get(const.FIELD_TYPE, None)
        self.resolution = element.get(const.FIELD_RESOLUTION, None)
        self.index = element.get(const.FIELD_INDEX, None)
        self.raw_elements = element.get(const.FIELD_ELEMENTS, [])
        self.elements = None
        if self.element_type in const.COMPLEX_TYPES:
            self.elements = Elements(self.element_type, self.raw_elements)

    @property
    def _type_is_ok(self) -> bool:
        """Does this element have a valid type?"""
        if self.element_type in const.ALL_TYPES:
            return True
        return False

    @property
    def _resolution_is_ok(self) -> bool:
        """Is the resolution field ok"""
        if self.element_type not in const.DATETIME_TYPES:
            return True
        return self.resolution in const.RESOLUTION_MAP

    @property
    def _index_is_ok(self) -> bool:
        """is the index field ok"""
        if not self.index:
            return True
        return self.is_indexable

    @property
    def normal_resolution(self) -> str | None:
        """the normalized resolution of this element"""
        if not self.resolution:
            return self.resolution
        return const.RESOLUTION_MAP.get(self.resolution, None)

    @property
    def view_type(self) -> str | None:
        """the view type of this element"""
        if self.element_type in const.BOOL_TYPES:
            return const.TYPE_UINT8
        if self.element_type in const.TRANSFORM_DATETIME_TYPES:
            if self.normal_resolution:
                return const.DATETIME_VIEW_TYPE_MAP.get(
                    self.normal_resolution, self.element_type
                )
        return self.element_type

    @property
    def element_types(self) -> set:
        """Get the types of all elements"""
        retval = set()
        if self.elements:
            for element in self.elements.elements:
                if element.element_type:
                    retval.add(element.element_type)
        return retval

    @property
    def is_indexable(self) -> bool:
        """Can this element be indexed?"""
        if self.element_type in const.ALWAYS_INDEXABLE_TYPES:
            return True
        if self.element_type in const.COMPLEX_TYPES:
            acceptable_subtypes = set(
                const.COMPLEX_INDEXABLE_SUBTYPES[self.element_type]
            )
            if self.element_types <= acceptable_subtypes:
                return True
        return False

    @property
    def repair_is_necessary(self) -> bool:
        """Does this element have any issues that need repair?"""
        return not all([self._type_is_ok, self._resolution_is_ok, self._index_is_ok])

    @property
    def repair_is_possible(self) -> bool:
        """elements can’t be repaired so only return true if they are in good shape"""
        return not self.repair_is_necessary

    def report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if not self._type_is_ok:
            report.add_message(
                logging.ERROR, "Element has invalid type '{self.element_type}'"
            )
        if not self._resolution_is_ok:
            report.add_message(
                logging.ERROR,
                "Element is type '{self.element_type}', but resolution '{self.resolution}' is invalid",
            )
        if not self._index_is_ok:
            report.add_message(
                logging.ERROR,
                "Element has index set, but can not be indexed",
            )
        if self.element_type in const.COMPLEX_TYPES:
            if self.elements:
                report = self.elements.report(report)

        return report

    @property
    def normalized_representation(self) -> dict:
        """get a normalized representation of this element for conflict comparison"""
        properties = [
            (const.FIELD_TYPE, self.view_type),
            (const.FIELD_RESOLUTION, self.normal_resolution),
            (const.FIELD_INDEX, self.index),
        ]
        if self.element_type in const.COMPLEX_TYPES:
            if self.elements:
                properties.append(
                    (const.FIELD_ELEMENTS, self.elements.normalized_conflict_elements)
                )
        retval = {}
        for field, value in properties:
            if field in self.raw_element:
                retval[field] = value
        return retval


class Elements:
    """Elements on a complex column"""

    def __init__(
        self,
        type: str,
        elements: list[dict],
    ) -> None:
        self.element_type = type
        self.raw_elements = elements or []
        self.elements = [Element(e) for e in self.raw_elements]

    @property
    def repair_is_possible(self) -> bool:
        """elements can’t be repaired"""
        return False

    @property
    def repair_is_necessary(self) -> bool:
        """do these elements need repair?"""
        checks = [e.repair_is_necessary for e in self.elements]
        checks.append(not self.element_count_is_correct)
        return any(checks)

    @property
    def element_count(self) -> int:
        """how many elements are there?"""
        return len(self.elements)

    @property
    def correct_element_count(self) -> int:
        """how many elements should there be?"""
        return const.COMPLEX_ELEMENT_COUNTS.get(self.element_type, 0)

    @property
    def element_count_is_correct(self) -> bool:
        return self.element_count == self.correct_element_count

    def report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if not self.element_count_is_correct:
            report.add_message(
                logging.ERROR,
                f"Column of type '{self.element_type}' has {self.element_count} elements, "
                f"should have {self.correct_element_count}",
            )
        for element in self.elements:
            report = element.report(report)
        return report

    @property
    def normalized_conflict_elements(self):
        """Normalize elements to a form that can be directly compared for conflict check"""
        return [e.normalized_representation for e in self.elements]
