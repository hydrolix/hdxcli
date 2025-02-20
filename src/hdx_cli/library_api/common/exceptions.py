class HdxCliException(Exception):
    "Base class for all exceptions"


class HttpException(HdxCliException):
    "Exception thrown from problems in Http calls to the config API"

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


class ConfigurationNotFoundException(HdxCliException):
    pass


class ConfigurationExistsException(HdxCliException):
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


class CatalogException(HdxCliException):
    pass


class MigrationException(HdxCliException):
    """
    Base exception class for Migration errors.
    """


class MigrationFailureException(MigrationException):
    def __init__(self, message):
        super().__init__(message)


class RCloneRemoteException(MigrationException):
    def __init__(self, message):
        super().__init__(message)


class RCloneRemoteCreationException(RCloneRemoteException):
    def __init__(self, bucket_name, cloud):
        message = f"Error creating remote connection to {bucket_name} ({cloud})."
        super().__init__(message)


class RCloneRemoteCheckException(RCloneRemoteException):
    def __init__(self, bucket_name, cloud):
        message = f"Error checking remote connection to {bucket_name} ({cloud})."
        super().__init__(message)
