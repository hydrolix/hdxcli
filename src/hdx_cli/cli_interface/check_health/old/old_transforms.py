# type: ignore
import logging

from . import columns, const


class TransformDataType(columns.DataType):
    @property
    def corrected_datatype(self) -> dict:
        """return a corrected version of this datatype if possible"""
        # Unimplemented
        return {}

    def _type_report(self, report, name):
        if self.type not in const.TRANSFORM_TYPES:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' type is '{self.type}', which is not a valid transform type",
            )

        if self.type != self.correct_type:
            report.add_message(
                logging.ERROR,
                f"Column '{name}' type is {self.type}, should be {self.correct_type}",
            )
        return report

    def _resolution_report(self, report, name):
        if self.type in const.TRANSFORM_DATETIME_TYPES:
            if not self.resolution:
                report.add_message(
                    logging.ERROR,
                    f"Column '{name}' type is '{self.type}', resolution is required, but not set",
                )
                return report
        return report


class TransformColumn(columns.Column):
    def __init__(self, name: str | None = "", datatype: dict | None = {}):
        self.name = name or ""
        self.raw_datatype = datatype or {}
        self.datatype = TransformDataType(**self.raw_datatype)
