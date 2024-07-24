

class HdxCliException(Exception):
   'Base class for all exceptions'


class HttpException(HdxCliException):
    'Exception thrown from problems in Http calls to the config API'
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message


class CommandLineException(HdxCliException):
    pass


class ActionNotAvailableException(CommandLineException):
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


class ProfileExistsException(HdxCliException):
    pass


class InvalidHostnameException(HdxCliException):
    pass


class InvalidUsernameException(HdxCliException):
    pass


class InvalidSchemeException(HdxCliException):
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


class QueryOptionNotFound(HdxCliException):
    pass


class StorageNotFoundError(HdxCliException):
    pass


class ProviderError(HdxCliException):
    """
    Base exception class for Provider errors.
    """
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CredentialsNotFoundError(ProviderError):
    def __init__(self, message="Credentials not found."):
        super().__init__(message)


class InvalidCredentialsError(ProviderError):
    def __init__(self, message="Invalid credentials."):
        super().__init__(message)


class CloudConnectionError(ProviderError):
    def __init__(self, message="There was an error connecting to the cloud service."):
        super().__init__(message)


class ProviderClassNotFoundError(ProviderError):
    def __init__(self, message):
        super().__init__(message)


class CatalogException(HdxCliException):
    pass
