import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import click
from requests import JSONDecodeError

from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.exceptions import (
    ActionNotAvailableException,
    HdxCliException,
    LogicException,
    ResourceNotFoundException,
)
from ...library_api.common.logging import get_logger
from ...library_api.utility.functions import heuristically_get_resource_kind
from ...models import AuthInfo, ProfileUserContext
from .cached_operations import *  # pylint:disable=wildcard-import,unused-wildcard-import

logger = get_logger()
DEFAULT_INDENTATION = 4


def basic_get(
    profile: ProfileUserContext,
    resource_path: str,
    *,
    fmt: str = "json",
    **params,
) -> Any:
    """
    Retrieves a resource.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path of the resource.
        fmt (str, optional): Response format. Defaults to "json".
        **params: Additional query parameters.

    Raises:
        HttpException: If the request fails.

    Returns:
        Any: Response from the request.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }
    return rest_ops.get(url, headers=headers, timeout=timeout, fmt=fmt, params=params)


def basic_create(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: Optional[str] = None,
    *,
    body: Optional[Union[str, bytes, dict]] = None,
    body_type: str = "json",
    extra_headers: Optional[dict] = None,
    **params,
) -> Any:
    """
    Creates a resource.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path to create the resource.
        resource_name (str, optional): Name of the resource.
        body (str|bytes|dict, optional): Resource content.
        body_type (str, optional): Type of the content if body is a string. Defaults to "json".
        extra_headers (dict, optional): Additional request headers.
        **params: Additional query parameters.

    Raises:
        HttpException: If the request fails.

    Returns:
        Any.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth

    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }

    if isinstance(body, dict):
        request_body = body
        if resource_name:
            request_body["name"] = resource_name

    elif isinstance(body, str):
        if body_type == "json":
            headers["Content-Type"] = "application/json"
            request_body = json.loads(body)
            if resource_name:
                request_body["name"] = resource_name
        else:
            if body_type == "csv":
                headers["Content-Type"] = "application/CSV"
                headers["Accept"] = "*/*"
            request_body = body

    elif isinstance(body, bytes):
        request_body = body

    elif not body and resource_name:
        headers["Content-Type"] = "application/json"
        request_body = {"name": resource_name}
    else:
        request_body = None

    if extra_headers:
        headers.update(extra_headers)

    return rest_ops.post(
        url,
        body=request_body,
        headers=headers,
        body_type=body_type,
        timeout=timeout,
        params=params,
    )


def basic_create_file(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: str = None,
    *,
    file_content: bytes,
    file_type: str = "json",
    extra_headers: Optional[dict] = None,
    **params,
) -> None:
    """
    Creates a resource from file content.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path to create the resource.
        resource_name (str, optional): Name of the resource.
        file_content (bytes): Content of the file.
        file_type (str, optional): Type of the file. Defaults to "json".
        extra_headers (dict, optional): Additional request headers.
        **params: Additional query parameters.

    Raises:
        HttpException: If the request fails.

    Returns:
        None.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }

    if extra_headers:
        headers.update(extra_headers)

    rest_ops.post_with_file(
        url,
        headers=headers,
        file_content=file_content,
        file_name=resource_name,
        timeout=timeout,
        params=params,
    )


def basic_update(
    profile: ProfileUserContext,
    resource_path: str,
    *,
    body: Union[str, bytes, dict],
    resource_name: Optional[str] = None,
    filter_field: str = "name",
    **params,
) -> None:
    """
    Updates a resource.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path of the resource.
        body (str|bytes|dict): Resource content.
        resource_name (str, optional): Name of the resource.
        filter_field (str, optional): Field to filter the resource. Defaults to "name".
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        None.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }

    if resource_name:
        # Get the resource ID from the resource name and update the URL.
        resource = json.loads(
            basic_show(
                profile, resource_path, resource_name, filter_field=filter_field, params=params
            )
        )
        resource_id = resource.get("uuid", resource.get("id", None))

        if not resource_id:
            _, resource_kind = heuristically_get_resource_kind(resource_path)
            raise ResourceNotFoundException(
                f"{resource_kind} with {filter_field} '{resource_name}' not found."
            )
        url += f"{resource_id}/"

    if isinstance(body, dict):
        request_body = body
    elif isinstance(body, str):
        request_body = json.loads(body)
    elif isinstance(body, bytes):
        request_body = body
    else:
        raise ValueError("Invalid body type")

    rest_ops.put(url, headers=headers, body=request_body, timeout=timeout, params=params)


def basic_show(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: str,
    *,
    indent: Optional[bool] = False,
    filter_field: Optional[str] = "name",
    **params,
) -> str:
    """
    Retrieves and returns a specific resource.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path of the resource.
        resource_name (str): Name of the resource.
        indent (bool, optional): Indent JSON output. Defaults to False.
        filter_field (str, optional): Field to filter the resource. Defaults to "name".
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        str: JSON string of the resource.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }

    indentation = DEFAULT_INDENTATION if indent else None
    has_next = True
    while has_next:
        response = rest_ops.get(list_url, headers=headers, timeout=timeout, params=params)

        # If the response is paginated, it should contain the "results" key.
        if "results" in response:
            resources = response.get("results", [])
        else:
            # If not paginated, assume the response is the list of resources.
            resources = response

        for resource in resources:
            if resource.get(filter_field) == resource_name:
                return json.dumps(resource, indent=indentation)

        # If the response is not paginated, break out of the loop.
        if "results" not in response:
            break

        # Pagination handling:
        # Update the "page" parameter to get the next page if available.
        next_page = response.get("next", 0)
        current_page = response.get("current", 0)
        num_pages = response.get("num_pages", 0)

        has_next = next_page != 0 and current_page < num_pages
        if not params:
            params = {"page": next_page}
        else:
            params["page"] = next_page

    _, resource_kind = heuristically_get_resource_kind(resource_path)
    message = (
        f"{resource_kind.capitalize()} with {filter_field} '{resource_name}' not found."
        if resource_name is not None
        else "Resource not found."
    )
    raise ResourceNotFoundException(message)


def _prepare_table_subresource_context(
        ctx: click.Context,
        plural_resource_name: str,  # e.g., "transforms", "views"
        singular_resource_name: str,  # e.g., "transform", "view"
):
    """Prepare the context for table subresource."""
    # Common logic to fetch profile, project, and table details
    profile: ProfileUserContext = ctx.parent.obj["usercontext"]
    project_name, table_name = profile.projectname, profile.tablename

    if not project_name or not table_name:
        raise HdxCliException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{profile.profilename}'"
        )

    org_id = profile.org_id
    projects_path = f"/config/v1/orgs/{org_id}/projects/"
    try:
        project_id = json.loads(basic_show(profile, projects_path, project_name))["uuid"]
    except IndexError as exc:
        raise ResourceNotFoundException(f"Project with name '{project_name}' not found.") from exc

    tables_path_prefix = f"/config/v1/orgs/{org_id}/projects/{project_id}/tables/"
    try:
        table_id = json.loads(basic_show(profile, tables_path_prefix, table_name))["uuid"]
    except IndexError as exc:
        raise ResourceNotFoundException(f"Table with name '{table_name}' not found.") from exc

    # Construct the specific "subresource" path
    resource_path = f"{tables_path_prefix}{table_id}/{plural_resource_name}/"
    ctx.obj = {"resource_path": resource_path, "usercontext": profile}

    # Check for specific subresource name if provided in profile
    specific_subresource_name = getattr(profile, f"{singular_resource_name}name", None)
    if not specific_subresource_name:
        return

    try:
        # Validate subresource existence
        _ = json.loads(basic_show(profile, resource_path, specific_subresource_name))["uuid"]
    except IndexError as exc:
        raise ResourceNotFoundException(
            f"{singular_resource_name.capitalize()} with name '{specific_subresource_name}' not found."
        ) from exc


def basic_transform(ctx: click.Context):
    _prepare_table_subresource_context(
        ctx,
        plural_resource_name="transforms",
        singular_resource_name="transform",
    )


def basic_view(ctx: click.Context):
    _prepare_table_subresource_context(
        ctx,
        plural_resource_name="views",
        singular_resource_name="view",
    )


class KeyAbsent:
    """Show absent key into the settings output"""

    def __str__(self):
        return "(Key absent)"


def _get_dotted_key_from_dict(dotted_key, the_dict):
    key_path = dotted_key.split(".")
    val = the_dict[key_path[0]]
    if len(key_path) > 1:
        for key_piece in key_path[1:]:
            if val is None:
                return KeyAbsent()
            val = val[key_piece]
    return val


def _do_create_dict_from_dotted_key_and_value(split_key, value, the_dict):
    if len(split_key) == 1:
        the_dict[split_key[0]] = value
        return
    the_dict[split_key[0]] = {}
    _do_create_dict_from_dotted_key_and_value(split_key[1:], value, the_dict[split_key[0]])


def _create_dict_from_dotted_key_and_value(dotted_key, value):
    the_dict = {}
    split_key = dotted_key.split(".")
    if len(split_key) == 1:
        return {dotted_key: value}
    _do_create_dict_from_dotted_key_and_value(split_key, value, the_dict)
    return the_dict


def _wrap_str(contents, prefix, suffix):
    return prefix + contents + suffix


def _format_key_val(key: str, val):
    return f"{key}:{_format_elem(val, obj_detailed=False)}"


def _format_list(lst, nelems=5):
    max_index = min(nelems, len(lst))
    result = []
    for val in lst[0:max_index]:
        result.append(_format_elem(val, obj_detailed=False))
    if max_index < len(lst):
        result.append("...")
    return _wrap_str(", ".join(result), "[", f"] ({len(lst)} elements)")


def _format_dict(dic, nelems=4, detailed=True):
    if not detailed:
        return "{...}"
    sorted_elems = sorted(dic.items())
    max_index = min(nelems, len(sorted_elems))
    result = []
    for key, val in sorted_elems[0:max_index]:
        result.append(_format_key_val(key, val))
    if max_index < len(sorted_elems):
        result.append("...")
    return _wrap_str(", ".join(result), "{", f"}} ({len(sorted_elems)} keys)")


def _format_elem(elem, obj_detailed=True):
    if isinstance(elem, list):
        return _format_list(elem)
    if isinstance(elem, dict):
        return _format_dict(elem, detailed=obj_detailed)
    if isinstance(elem, KeyAbsent):
        return "(Key absent)"
    return json.dumps(elem)


def _format_setting(dotted_key, value, resource_value):
    return f"{dotted_key:<90}{value:<30}{_format_elem(resource_value):<40}"


def _format_settings_header(headers_and_spacing: List[Tuple[str, int]]):
    format_strings = []
    for key, spacing in headers_and_spacing:
        format_strings.append(f"{key:<{spacing}}")
    return "".join(format_strings)


def _do_for_each_setting(settings_dict, prefix="", resource=None):
    for setting_name, setting_val in settings_dict.items():
        if setting_val.get("read_only"):
            continue
        if setting_val.get("type") == "nested object" and setting_val.get("children"):
            the_prefix = prefix + "." if prefix else ""
            settings_dict = setting_val.get("children")
            _for_each_setting(settings_dict, the_prefix + setting_name, resource)
        else:
            full_key_name = setting_name if not prefix else prefix + "." + setting_name
            the_value_in_resource = None
            try:
                the_value_in_resource = _get_dotted_key_from_dict(full_key_name, resource)
            except KeyError:
                the_value_in_resource = KeyAbsent()
            logger.info(
                _format_setting(full_key_name, setting_val.get("type"), the_value_in_resource)
            )


def _for_each_setting(settings_dict, prefix="", resource=None):
    _do_for_each_setting(settings_dict, prefix, resource)


DottedKey = str


def _settings_update(resource: Dict[str, Any], key: DottedKey, value: Any):
    """
    Update resource and return it with updated_data
    """
    key_parts = key.split(".")
    the_value = None
    try:
        the_value = json.loads(value)
    except json.JSONDecodeError:
        the_value = value
    resource_key = resource
    for k in key_parts[0:-1]:
        resource_key = resource_key[k]

    resource_key[key_parts[-1]] = the_value
    return resource


def log_formatted_table_header(headers_and_spacing: Dict[str, int]) -> None:
    format_strings = []
    values = headers_and_spacing.values()

    logger.info(f'{"-" * sum(values)}')

    for key, spacing in headers_and_spacing.items():
        format_strings.append(f"{key:<{spacing}}")

    logger.info(f'{"".join(format_strings)}')
    logger.info(f'{"-" * sum(values)}')


def basic_settings(
    profile: ProfileUserContext, resource_path: str, key: str, value: Any, **params
) -> None:
    """
    Three cases:
    1. key is None: show all settings
    2. key is not None and value is None: show the value of key
    3. key is not None and value is not None: update the value of key

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path.
        key (str): Key to show or update.
        value (Any): Value to update the key with.
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        None.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    settings_url = f"{scheme}://{hostname}{resource_path}"
    auth = profile.auth
    headers = {"Authorization": f"{auth.token_type} {auth.token}", "Accept": "application/json"}

    structure_resource_settings = basic_options(profile, resource_path)

    resource_kind_plural, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + "name")):
        raise LogicException(f"No default {resource_kind} found in profile")
    resources = None

    try:
        resources = globals()["find_" + resource_kind_plural](profile)
        resource = [r for r in resources if r["name"] == resource_name][0]
    except IndexError as idx_err:
        raise ResourceNotFoundException(
            f"{resource_kind.capitalize()} with name '{resource_name}' not found."
        ) from idx_err

    if not key:
        log_formatted_table_header({"name": 90, "type": 30, "value": 40})
        _for_each_setting(structure_resource_settings, resource=resource)
    elif key and not value:
        try:
            logger.info(f"{key}: {_get_dotted_key_from_dict(key, resource)}")
        except KeyError:
            logger.info(f"Key '{key}' not found in {resource['name']}.")
    else:
        this_resource_url = f"{settings_url}{resource['uuid']}"
        try:
            resource = _settings_update(resource, key, value)
            rest_ops.put(
                this_resource_url, headers=headers, timeout=timeout, body=resource, params=params
            )
        except Exception as exc:
            logger.debug(f"Error updating resource settings using PUT: {exc}")
            logger.debug("Trying to update using PATCH")
            patch_data = _create_dict_from_dotted_key_and_value(key, value)
            rest_ops.patch(
                this_resource_url, headers=headers, timeout=timeout, body=patch_data, params=params
            )
        logger.info(f"Updated {resource['name']} {key}")


def basic_delete(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: str,
    *,
    filter_field: str = "name",
    url: Optional[str] = None,
    **params,
) -> bool:
    """
    Deletes a resource.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Path of the resource.
        resource_name (str): Name of the resource.
        filter_field (str, optional): Field to filter the resource. Defaults to "name".
        url (str, optional): URL of the resource. Defaults to None.
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        bool: True if the resource was deleted, False otherwise.
    """
    if not url:
        resource = json.loads(
            basic_show(
                profile, resource_path, resource_name, filter_field=filter_field, params=params
            )
        )
        resource_id = resource.get("uuid", resource.get("id", None))

        scheme = profile.scheme
        hostname = profile.hostname
        url = f"{scheme}://{hostname}{resource_path}{resource_id}" if resource_id else None
        if not url:
            _, resource_kind = heuristically_get_resource_kind(resource_path)
            logger.debug(f"Error building URL for {resource_kind} '{resource_name}'.")
            return False

    timeout = profile.timeout
    auth = profile.auth
    headers = {"Authorization": f"{auth.token_type} {auth.token}", "Accept": "application/json"}
    rest_ops.delete(url, headers=headers, timeout=timeout, params=params)
    return True


def basic_list(
    profile: ProfileUserContext,
    resource_path: str,
    *,
    filter_field: Optional[str] = "name",
    **params,
) -> None:
    """
    List resources using the provided data. If the resources are paginated, it shows
    the current page, the total number of pages available and the total number of resources.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path to list.
        filter_field (str, optional): Field to filter the resource. Defaults to "name".
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        None.
    """
    response = generic_basic_list(profile, resource_path, **params)

    count, current_count, current, num_pages = None, None, None, None
    # If the response is paginated, it should contain the "results" key.
    if "results" in response:
        resources = response.get("results", [])
        count = response.get("count", 0)
        current_count = len(resources)
        current = response.get("current", 0)
        num_pages = response.get("num_pages", 0)
    else:
        # If not paginated, assume the response is the list of resources.
        resources = response

    for resource in resources:
        if isinstance(resource, str):
            logger.info(resource)
        else:
            if (settings := resource.get("settings")) and settings.get("is_default"):
                logger.info(f"{resource[filter_field]} (default)")
            else:
                logger.info(f"{resource[filter_field]}")

    if count not in (None, 0):
        plural, singular = heuristically_get_resource_kind(resource_path)
        resource_name = plural if count > 1 else singular
        logger.info(
            f"Listed {current_count} of {count} {resource_name} [page {current}/{num_pages}]"
        )


def generic_basic_list(
    profile: ProfileUserContext,
    resource_path: str,
    **params,
) -> dict:
    """
    Return a list of resources using the provided data.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path to list.
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        dict: Response from the request.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }
    return rest_ops.get(url, headers=headers, timeout=timeout, params=params)


def basic_stats(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: str,
    *,
    indent: Optional[bool] = False,
    **params,
) -> None:
    """
    Get and display the statistics of a resource using the provided data.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path to show.
        resource_name (str): Resource name.
        indent (bool, optional): Indent JSON output. Defaults to False.
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        None.
    """
    resource = json.loads(basic_show(profile, resource_path, resource_name))
    resource_id = resource.get("uuid", resource.get("id", None))

    hostname = profile.hostname
    scheme = profile.scheme
    url = f"{scheme}://{hostname}{resource_path}{resource_id}/stats" if resource_id else None
    if not url:
        _, resource_kind = heuristically_get_resource_kind(resource_path)
        raise ResourceNotFoundException(
            f"There was an error building the URL for the {resource_kind} '{resource_name}'."
        )

    timeout = profile.timeout
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }
    stats = rest_ops.get(url, headers=headers, timeout=timeout, params=params)

    logger.info(json.dumps(stats, indent=DEFAULT_INDENTATION if indent else None))


def _format_activities(activities: list) -> list:
    simplified = []
    for act in activities:
        timestamp = act.get("created")
        user = act.get("log", {}).get("user", {}).get("username", "unknown")
        action = act.get("action", "unknown")

        try:
            timestamp_formatted = datetime.fromisoformat(timestamp.rstrip("Z")).strftime(
                "%Y-%m-%d %H:%M"
            )
        except (ValueError, TypeError, AttributeError):
            timestamp_formatted = "invalid date"

        simplified.append({"timestamp": timestamp_formatted, "user": user, "action": action})

    return simplified


def basic_activity(
    profile: ProfileUserContext,
    resource_path: str,
    resource_name: str,
    **params,
) -> None:
    """
    Get and display the activity of a resource using the provided data. If the resource is paginated,
    it shows the current page, the total number of pages available and the total number of activities.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path to show.
        resource_name (str): Resource name.
        **params: Additional query parameters.

    Raises:
        ResourceNotFoundException: If the resource is not found.

    Returns:
        None.
    """
    resource = json.loads(basic_show(profile, resource_path, resource_name))
    resource_id = resource.get("uuid", resource.get("id", None))

    hostname = profile.hostname
    scheme = profile.scheme
    url = f"{scheme}://{hostname}{resource_path}{resource_id}/activity"
    if not url:
        _, resource_kind = heuristically_get_resource_kind(resource_path)
        raise ResourceNotFoundException(
            f"There was an error building the URL for the {resource_kind} '{resource_name}'."
        )

    timeout = profile.timeout
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }
    response = rest_ops.get(url, headers=headers, timeout=timeout, params=params)

    count, current_count, current, num_pages = None, None, None, None
    # If the response is paginated, it should contain the "results" key.
    if "results" in response:
        activities = response.get("results", [])
        count = response.get("count", 0)
        current_count = len(activities)
        current = response.get("current", 0)
        num_pages = response.get("num_pages", 0)
    else:
        # If not paginated, assume the response is the list of resources.
        activities = response

    if not activities:
        return

    simplified_activities = _format_activities(activities)
    logger.info(f'{"-" * (20 + 35 + 25)}')
    logger.info(_format_settings_header([("created", 20), ("user", 35), ("action", 25)]))
    logger.info(f'{"-" * (20 + 35 + 25)}')
    for act in simplified_activities:
        logger.info(f"{act['timestamp']:19} {act['user']:34} {act['action']:24}")

    if count is not None:
        logger.info("")
        logger.info(f"Showed {current_count} of {count} activities [page {current}/{num_pages}]")


def basic_options(profile: ProfileUserContext, resource_path: str, action: str = "POST") -> dict:
    """
    Get the available options for a resource using the provided data. The options are the actions
    that can be performed on the resource. In this case, the action is "POST" by default.

    Args:
        profile (ProfileUserContext): User profile context.
        resource_path (str): Resource path to show.
        action (str, optional): Action to perform. Defaults to "POST".

    Raises:
        ActionNotAvailableException: If the action is not available on the resource.
        HttpException: If the request fails.

    Returns:
        dict: The available options for the resource.
    """
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f"{scheme}://{hostname}{resource_path}"
    auth_info: AuthInfo = profile.auth
    headers = {
        "Authorization": f"{auth_info.token_type} {auth_info.token}",
        "Accept": "application/json",
    }

    response = rest_ops.options(url, headers=headers, timeout=timeout)
    try:
        options = response.json()
        return options["actions"][action]
    except (JSONDecodeError, KeyError, TypeError) as exc:
        logger.debug(f"Error getting options for resource: {exc}")
        raise ActionNotAvailableException(
            "The 'settings' action is not available on this resource."
        )
