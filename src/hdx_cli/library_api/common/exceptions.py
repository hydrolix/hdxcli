

class HdxCliException(Exception):
   'Base class for all exceptions'


class HttpException(HdxCliException):
    'Exception thrown from problems in Http calls to the config API'
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message


class CommandLineException(HdxCliException):
    pass


class BadFileNameConventionException(HdxCliException):
    pass


class NotImplementedException(HdxCliException):
    pass


class NotSupportedException(HdxCliException):
    pass


class TokenExpiredException(HdxCliException):
    pass


class LoginException(HdxCliException):
    pass


class CacheFileNotFoundException(HdxCliException, FileNotFoundError):
    pass


class ProfileNotFoundException(HdxCliException):
    pass


# A resource like a project, transform, etc. was not found
class ResourceNotFoundException(HdxCliException):
    pass


class ProjectNotFoundException(ResourceNotFoundException):
    pass


class TableNotFoundException(ResourceNotFoundException):
    pass


class TransformNotFoundException(ResourceNotFoundException):
    pass


class TransformFileNotFoundException(ResourceNotFoundException):
    pass


class LogicException(HdxCliException):
    pass


class InvalidFormatFileException(HdxCliException):
    pass


class MissingSettingsException(InvalidFormatFileException):
    pass


class InvalidRoleException(HdxCliException):
    pass


class InvalidDataException(HdxCliException):
    pass


class InvalidEmailException(InvalidDataException):
    pass
