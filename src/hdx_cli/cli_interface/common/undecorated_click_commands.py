import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import click
from requests import JSONDecodeError
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

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
console = Console()
DEFAULT_INDENTATION = 4


@dataclass
class PaginatedResponse:
    """A normalized structure for API list responses."""
    results: List[Dict[str, Any]]
    count: int
    current_page: int
    num_pages: int


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
    indentation = DEFAULT_INDENTATION if indent else None
    current_params = params.copy() if params else {}

    while True:
        raw_response = generic_basic_list(profile, resource_path, **current_params)
        paginated_response = _get_paginated_resources(raw_response)

        for resource in paginated_response.results:
            if resource.get(filter_field) == resource_name:
                return json.dumps(resource, indent=indentation)

        # Decide if we should fetch the next page
        if paginated_response.current_page < paginated_response.num_pages:
            current_params["page"] = paginated_response.current_page + 1
        else:
            # No more pages to check
            break

    # If the loop completes without finding the resource
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

    # Validate subresource existence
    specific_subresource = json.loads(basic_show(profile, resource_path, specific_subresource_name))
    if not specific_subresource:
        raise ResourceNotFoundException(
            f"{singular_resource_name.capitalize()} with name '{specific_subresource_name}' not found."
        )

    # Store the found resource object in the context
    ctx.obj["specific_resource"] = specific_subresource


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


def basic_column(ctx: click.Context):
    _prepare_table_subresource_context(
        ctx,
        plural_resource_name="columns",
        singular_resource_name="column",
    )


def basic_row_policy(ctx: click.Context):
    _prepare_table_subresource_context(
        ctx,
        plural_resource_name="rowpolicies",
        singular_resource_name="rowpolicy",
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
    raw_response = generic_basic_list(profile, resource_path, **params)
    paginated_response = _get_paginated_resources(raw_response)

    if not paginated_response.results:
        plural, _ = heuristically_get_resource_kind(resource_path)
        logger.info(f"No {plural} found.")
        return

    table = Table(box=None, show_header=True, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name")

    for resource in paginated_response.results:
        if isinstance(resource, str):
            table.add_row(resource)
        else:
            name = resource.get(filter_field, "[N/A]")
            if (settings := resource.get("settings")) and settings.get("is_default"):
                table.add_row(f"{name} (default)")
            else:
                table.add_row(name)

    console.print(table)

    if paginated_response.count > 0:
        plural, singular = heuristically_get_resource_kind(resource_path)
        resource_name = plural if paginated_response.count != 1 else singular

        # Build the footer message
        footer_text = Text(style="dim")
        footer_text.append(
            f"Listed {len(paginated_response.results)} of {paginated_response.count} {resource_name}"
        )
        if paginated_response.num_pages > 1:
            footer_text.append(f" [page {paginated_response.current_page}/{paginated_response.num_pages}]")

        console.print(footer_text)


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


_KEY_LABELS = {
    "name": "Name",
    "total_partitions": "Total Partitions",
    "total_rows": "Total Rows",
    "total_data_size": "Total Data (bytes)",
    "total_storage_size": "Total Storage (bytes)",
    "total_raw_data_size": "Total Raw Data (bytes)",
}


def _create_stats_table(stats_dict: Dict[str, Any]) -> Table:
    """Creates a rich Table for displaying statistics."""
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("Key", style="dim")
    table.add_column("Value")
    for key, value in stats_dict.items():
        label = _KEY_LABELS.get(key, key.replace('_', ' ').title())
        table.add_row(f"{label}:", str(value))
    return table


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

    if indent:
        logger.info(json.dumps(stats, indent=DEFAULT_INDENTATION))
        return

    # Handle project-specific stats format
    if "summary" in stats and "tables" in stats:
        console.print(_create_stats_table(stats["summary"]))
        for table_stats in stats.get("tables", []):
            console.print(Rule(style="dim"))
            console.print(_create_stats_table(table_stats))
    else:
        # Handle single-resource stats (like a table)
        console.print(_create_stats_table(stats))


def _get_activity_username(activity: Dict[str, Any]) -> str:
    """Extracts the username from different activity log structures."""
    # Path for table-like activities
    log = activity.get("log", {})
    username = (
        log.get("context", {}).get("user", {}).get("snapshot", {}).get("username")
    )
    if username:
        return username

    # Path for project-like activities
    username = log.get("user", {}).get("username")
    if username:
        return username

    # Direct path as a fallback
    username = activity.get("username")
    if username:
        return username

    return "unknown"


def _format_activities(activities: list) -> list:
    simplified = []
    for act in activities:
        timestamp = act.get("created")
        user = _get_activity_username(act)
        action = act.get("action", "unknown")

        try:
            dt_obj = datetime.fromisoformat(timestamp.rstrip("Z"))
            timestamp_formatted = dt_obj.strftime("%Y-%m-%d %H:%M")
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
    paginated_response = _get_paginated_resources(response)

    if not paginated_response.results:
        logger.info("No activity found.")
        return

    simplified_activities = _format_activities(paginated_response.results)

    table = Table(box=None, show_header=True, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Created", style="dim")
    table.add_column("User")
    table.add_column("Action", overflow="fold")

    for act in simplified_activities:
        table.add_row(act["timestamp"], act["user"], act["action"])

    console.print(table)

    if paginated_response.count > 0:
        footer_text = Text(style="dim")
        footer_text.append(
            f"Listed {len(paginated_response.results)} of {paginated_response.count} activities"
        )
        if paginated_response.num_pages > 1:
            footer_text.append(f" [page {paginated_response.current_page}/{paginated_response.num_pages}]")
        console.print(footer_text)


def _get_paginated_resources(response: Dict[str, Any]) -> PaginatedResponse:
    """Normalizes a paginated or non-paginated API list response into a consistent object."""
    if "results" in response:
        # Standard paginated response
        return PaginatedResponse(
            results=response.get("results", []),
            count=response.get("count", 0),
            current_page=response.get("current", 1),
            num_pages=response.get("num_pages", 1),
        )
    # Non-paginated response (treat it as a single page)
    results = response if isinstance(response, list) else []
    return PaginatedResponse(
        results=results,
        count=len(results),
        current_page=1,
        num_pages=1,
    )


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
