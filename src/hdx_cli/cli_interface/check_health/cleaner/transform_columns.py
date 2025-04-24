import copy
import logging

from .. import const, reportlog
from . import columns, elements


class TransformColumn(columns.Column):
    def __init__(self, raw_column: dict, column_reference: dict | None = None) -> None:
        super().__init__(raw_column)
        self.column_reference = column_reference

    @property
    def repair_is_necessary(self) -> bool:
        """Does this column need repair?"""
        if self.is_missing_from_reference:
            return True
        if self.correct_type in const.COMPLEX_TYPES:
            if self.elements.repair_is_necessary:
                return True
        if self.has_primary_issue:
            return True
        if self.has_type_issue:
            return True
        if self.has_index_issue:
            return True
        if self.has_resolution_issue:
            return True
        return False

    @property
    def repair_is_possible(self) -> bool:
        """Can this column be repaired?"""
        if self.correct_type in const.COMPLEX_TYPES:
            if self.elements.repair_is_necessary:
                return False
        return not self.has_primary_issue

    @property
    def is_missing_from_reference(self):
        return not bool(self.column_reference)

    def _missing_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report columns with no reference"""
        if self.is_missing_from_reference:
            report.add_message(
                logging.WARNING, f"Column '{self.name}' is not present in the auto-view"
            )
        return report

    @property
    def has_type_issue(self) -> bool:
        if self.column_type not in const.TRANSFORM_TYPES:
            return True
        if self.view_type != self.correct_view_type:
            return True
        return False

    def _type_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report columns with incorrect type"""
        if self.column_type not in const.TRANSFORM_TYPES:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', which is not a valid transform column datatype",
            )
        if self.view_type != self.correct_view_type:
            if self.column_reference:
                autoview_type = self.column_reference.get(const.FIELD_TYPE)
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}' in conflict with autoview type '{autoview_type}'",
            )
        return report

    @property
    def has_index_conflict(self) -> bool:
        if self.column_reference:
            autoview_index = self.column_reference.get(
                const.FIELD_INDEX, self.is_indexable
            )
            if self.index != autoview_index:
                return True
        return False

    @property
    def has_index_issue(self) -> bool:
        if self.has_index_conflict:
            return True
        if not self.is_indexable and self.index:
            return True
        return False

    def _index_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        report = super()._index_report(report)
        if self.has_index_conflict:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' index is '{self.index}' which does not match autoview",
            )
        return report

    @property
    def has_resolution_conflict(self) -> bool:
        if self.column_reference:
            autoview_resolution = self.column_reference.get(const.FIELD_RESOLUTION)
            if self.resolution != autoview_resolution:
                return True
        return False

    @property
    def has_resolution_issue(self) -> bool:
        if self.column_type not in const.TRANSFORM_DATETIME_TYPES:
            return False
        if not self.resolution:
            return True
        if self.has_resolution_conflict:
            return True
        return False

    def _resolution_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if self.column_type not in const.TRANSFORM_DATETIME_TYPES:
            return report
        if not self.resolution:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}' resolution is required, but absent",
            )
        if self.has_resolution_conflict:
            if self.column_reference:
                autoview_resolution = self.column_reference.get(const.FIELD_RESOLUTION)
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' resolution is '{self.resolution}' which does not match autoview '{autoview_resolution}",
            )
        return report

    @property
    def has_element_conflict(self):
        if self.column_reference:
            raw_reference_elements = self.column_reference.get(
                const.FIELD_ELEMENTS, None
            )
            if raw_reference_elements:
                reference_elements = elements.Elements(
                    self.column_type, raw_reference_elements
                ).normalized_conflict_elements
                if self.elements.normalized_conflict_elements != reference_elements:
                    return True
        return False

    def _element_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if self.column_type not in const.COMPLEX_TYPES:
            return report
        report = super()._element_report(report)
        if self.has_element_conflict:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' elements in conflict with autoview",
            )
        return report

    @property
    def has_primary_conflict(self) -> bool:
        """Does this colum have a conflict on primary key?"""
        if self.column_reference:
            autoview_primary = self.column_reference.get(const.FIELD_PRIMARY, False)
            if self.primary != autoview_primary:
                return True
        return False

    @property
    def has_primary_issue(self) -> bool:
        """Does this column have any issue relating to primary field?"""
        if self.has_primary_conflict:
            return True
        if self.primary and not self.can_be_primary:
            return True
        return False

    def _primary_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """Report column with incorrect primary value"""
        report = super()._primary_report(report)
        if self.primary != self.correct_primary:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' primary is '{self.primary}' which does not match autoview",
            )
        return report

    def build_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        """generate a report of issues on this column"""
        report_functions = [
            self._missing_report,
            self._type_report,
            self._primary_report,
            self._index_report,
            self._resolution_report,
            self._element_report,
        ]
        for report_function in report_functions:
            report = report_function(report)
        return report

    @property
    def correct_primary(self) -> bool:
        """Should this column be primary?"""
        if self.column_reference:
            return self.column_reference.get(const.FIELD_PRIMARY) or False
        if self.can_be_primary:
            return self.primary
        return False

    @property
    def correct_index(self) -> bool:
        """What index should this column be?"""
        if self.column_reference:
            return self.column_reference.get(const.FIELD_INDEX) or self.is_indexable
        if (self.index is not None) and self.is_indexable:
            return self.index
        return self.is_indexable

    @property
    def view_type(self):
        """What is the auto-view type of this transform?"""
        if self.column_type in const.BOOL_TYPES:
            return const.TYPE_UINT8
        if self.column_type in const.TRANSFORM_DATETIME_TYPES:
            if self.resolution:
                return const.DATETIME_VIEW_TYPE_MAP.get(
                    self.resolution, const.RESOLUTION_SECOND
                )
            else:
                return const.DATETIME_VIEW_TYPE_MAP.get(const.RESOLUTION_SECOND)
        return self.column_type

    @property
    def correct_type(self) -> str | None:
        """What type should this column be?"""
        if self.column_reference:
            reference_type = self.column_reference.get(const.FIELD_TYPE)
            if self.view_type != reference_type:
                if reference_type in const.DATETIME_TYPES:
                    return const.TYPE_DATETIME
                return reference_type
        return self.column_type

    @property
    def correct_view_type(self) -> str | None:
        if self.correct_type in const.BOOL_TYPES:
            return const.TYPE_UINT8
        if self.correct_type in const.TRANSFORM_DATETIME_TYPES:
            if self.resolution:
                return const.DATETIME_VIEW_TYPE_MAP.get(
                    self.resolution, const.RESOLUTION_SECOND
                )
            else:
                return const.DATETIME_VIEW_TYPE_MAP.get(const.RESOLUTION_SECOND)
        return self.correct_type

    @property
    def correct_resolution(self) -> str | None:
        """What resolution should this column be?"""
        if self.column_reference:
            return self.column_reference.get(const.FIELD_RESOLUTION)
        return None

    @property
    def corrected_column(self) -> dict:
        correct_datatype = copy.copy(self.datatype)
        correct_datatype[const.FIELD_TYPE] = self.correct_type
        correct_datatype[const.FIELD_INDEX] = self.correct_index
        correct_datatype[const.FIELD_RESOLUTION] = self.correct_resolution
        correct_datatype[const.FIELD_PRIMARY] = self.correct_primary
        correct_column = copy.copy(self.raw_column)
        correct_column[const.FIELD_DATATYPE] = correct_datatype
        return correct_column
