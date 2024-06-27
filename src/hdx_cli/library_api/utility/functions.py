from typing import Tuple


def heuristically_get_resource_kind(resource_path) -> Tuple[str, str]:
    """Returns plural and singular names for resource kind given a resource path.
       If it is a nested resource
    For example:

          - /config/.../tables/ -> ('tables', 'table')
          - /config/.../projects/ -> ('projects', 'project')
          - /config/.../jobs/batch/ -> ('batch', 'batch')
    """
    split_path = resource_path.split("/")
    plural = split_path[-2]
    if plural == "dictionaries":
        return "dictionaries", "dictionary"
    if plural == 'kinesis':
        return 'kinesis', 'kinesis'
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular
