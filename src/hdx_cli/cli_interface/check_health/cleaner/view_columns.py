import copy
import logging

from .. import const, reportlog
from . import columns


class AutoViewColumn(columns.Column):
    @property
    def repair_is_necessary(self):
        """Does this column need repair"""
        type_is_wrong = self.correct_type != self.column_type
        index_is_wrong = self.correct_index != self.index
        element_is_wrong = self.elements.repair_is_necessary
        return any([type_is_wrong, index_is_wrong, element_is_wrong])

    @property
    def repair_is_possible(self):
        """Can this column be repaired?"""
        # elements issues are the only thing that make a column unrepairable
        if self.column_type not in const.COMPLEX_TYPES:
            return True
        return self.elements.repair_is_possible

    @property
    def correct_type(self):
        """Return the correct type for this column"""
        if self.column_type not in const.VIEW_DATETIME_TYPES:
            return self.column_type
        if self.resolution == const.RESOLUTION_MILLISECOND:
            return const.TYPE_DATETIME64
        return const.TYPE_DATETIME

    @property
    def correct_index(self):
        """Return the correct index value for this column"""
        if self.index is None:
            return self.is_indexable
        return bool(self.index) if self.is_indexable else False

    # Report Parts
    def _resolution_report(self, report):
        if self.column_type not in const.VIEW_DATETIME_TYPES:
            return report
        if not self.resolution:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', resolution is required, but not set",
            )
            return report
        is_64 = self.column_type == const.TYPE_DATETIME64
        is_ms = self.resolution == const.RESOLUTION_MILLISECOND
        if is_64 and not is_ms:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', but resolution is not 'ms'",
            )
        elif not is_64 and is_ms:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', but resolution is not 's'",
            )
        return report

    def _type_report(self, report: reportlog.ReportLog) -> reportlog.ReportLog:
        if self.column_type not in const.VIEW_TYPES:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is '{self.column_type}', which is not a valid view column datatype",
            )

        if self.column_type != self.correct_type:
            report.add_message(
                logging.ERROR,
                f"Column '{self.name}' type is {self.column_type}, should be {self.correct_type}",
            )
        return report

    def report(self, report):
        """Generate a report of issues on this tableschema"""
        report_functions = [
            self._type_report,
            self._resolution_report,
        ]
        for report_function in report_functions:
            report = report_function(report)
        return report

    # End Report

    @property
    def conflict_reference(self) -> dict:
        """return a dict that can be used for easy conflict resolution reference"""
        return {
            const.FIELD_TYPE: self.correct_type,
            const.FIELD_INDEX: self.correct_index,
            const.FIELD_ELEMENTS: self.raw_elements,
            const.FIELD_RESOLUTION: self.resolution,
            const.FIELD_PRIMARY: self.primary,
        }

    @property
    def corrected_column(self) -> dict:
        """return a corrected version of this column if possible"""
        correct_datatype = copy.copy(self.datatype)
        correct_datatype[const.FIELD_TYPE] = self.correct_type
        correct_datatype[const.FIELD_INDEX] = self.correct_index
        correct_column = copy.copy(self.raw_column)
        correct_column[const.FIELD_DATATYPE] = correct_datatype
        return correct_column
