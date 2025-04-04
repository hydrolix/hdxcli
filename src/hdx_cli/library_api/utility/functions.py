from typing import Tuple


def heuristically_get_resource_kind(resource_path: str) -> Tuple[str, str]:
    """
    Returns plural and singular names for resource kind given a resource path.
    For example:

          - /config/.../tables/ -> ('tables', 'table')
          - /config/.../projects/ -> ('projects', 'project')
          - /config/.../jobs/batch/ -> ('batch', 'batch')
    """
    # Ensure resource path ends with a slash
    if not resource_path.endswith("/"):
        resource_path += "/"

    split_path = resource_path.split("/")
    try:
        plural = split_path[-2]
    except IndexError:
        # No resource kind found, use default
        plural = "resources"

    if plural == "dictionaries":
        return "dictionaries", "dictionary"
    if plural == "kinesis":
        return "kinesis", "kinesis"
    singular = plural if not plural.endswith("s") else plural[0:-1]
    return plural, singular
