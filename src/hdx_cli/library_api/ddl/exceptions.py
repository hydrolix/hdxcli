from ...library_api.common.exceptions import HdxCliException


class IngestIndexError(HdxCliException):
    pass


class NoPrimaryKeyFoundException(HdxCliException):
    pass
