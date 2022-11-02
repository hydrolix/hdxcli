

class HdxCliException(Exception):
   'Base class for all exceptions'


class HttpException(Exception):
    'Exception thrown from problems in Http calls to the config API'


class BadFileNameConventionException(HdxCliException):
   pass


class NotImplementedException(HdxCliException):
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
