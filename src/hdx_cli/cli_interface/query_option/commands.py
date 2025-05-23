from typing import List, Tuple, Union

import click

from ...library_api.common.exceptions import HdxCliException, QueryOptionNotFound
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.undecorated_click_commands import basic_get, basic_options, basic_update

logger = get_logger()


@click.group(help="Query options operations at org-level", name="query-option")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def query_option(ctx: click.Context):
    profile = ctx.parent.obj["usercontext"]
    org_id = profile.org_id
    ctx.obj = {"resource_path": f"/config/v1/orgs/{org_id}/query_options/", "usercontext": profile}


@click.command(help="Set query option(s).", name="set")
@click.argument("query_option_name", default=None, required=False)
@click.argument("query_option_value", default=None, required=False)
@click.option(
    "--from-file",
    default=None,
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
    help="Set query options from a JSON file.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set_(
    ctx: click.Context, query_option_name: str, query_option_value: Union[str, int], from_file: dict
):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    if not (query_option_name and query_option_value) and not from_file:
        raise click.BadParameter(
            "You must provide either query_option_name and query_option_value or --from-file (JSON)."
        )

    response = _set(user_profile, resource_path, query_option_name, query_option_value, from_file)
    logger.info(f"{response}")


@click.command(help="Unset query option(s).")
@click.argument("query_option_name", default=None, required=False)
@click.option(
    "--all", "all_query_options", is_flag=True, default=False, help="Unset all query options."
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def unset(ctx: click.Context, query_option_name: str, all_query_options: bool):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    if query_option_name is None and not all_query_options:
        raise click.BadParameter("Either provide a QUERY_OPTION_NAME or --all option.")

    response = _unset(user_profile, resource_path, query_option_name=query_option_name)
    logger.info(f"{response}")


@click.command(help="List query options.", name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    _list(profile, resource_path)


def _set(
    profile: ProfileUserContext,
    resource_path: str,
    query_option_name: str = None,
    query_option_value: Union[str, int] = None,
    from_file: dict = None,
):
    if not (available_options := _available_query_options(profile, resource_path)):
        logger.error("There was an error catching available query options.")
        return

    result = basic_get(profile, resource_path)
    if not result.get("settings") or "default_query_options" not in result.get("settings"):
        raise HdxCliException("An error occurred while trying to get the query options.")

    if query_option_name:
        if query_option_name not in available_options:
            raise QueryOptionNotFound(f"'{query_option_name}' is not a valid query option.")

        result["settings"]["default_query_options"][query_option_name] = query_option_value
    else:
        if not all(key in available_options for key in from_file.keys()):
            raise QueryOptionNotFound("There are invalid query options in the file.")

        result["settings"]["default_query_options"].update(from_file)

    basic_update(profile, resource_path, body=result)

    return (
        f"Set '{query_option_name}' query option"
        if query_option_name
        else "Set query options from file"
    )


def _unset(profile: ProfileUserContext, resource_path: str, query_option_name: str = None) -> str:
    result = basic_get(profile, resource_path)
    default_query_options = result.get("settings", {}).get("default_query_options")
    if not default_query_options:
        return "No query options found to unset."

    data = result["settings"]
    try:
        if query_option_name:
            del data["default_query_options"][query_option_name]
        else:
            del data["default_query_options"]
    except KeyError as key_err:
        raise QueryOptionNotFound(
            f"{query_option_name} not found in the set query options."
        ) from key_err

    basic_update(profile, resource_path, body=result)
    return (
        f"Unset '{query_option_name}' query option"
        if query_option_name
        else "Unset all query options"
    )


def _list(profile: ProfileUserContext, resource_path: str) -> None:
    result = basic_get(profile, resource_path)
    default_query_options = result.get("settings", {}).get("default_query_options")
    if not default_query_options:
        return

    if not (available_options := _available_query_options(profile, resource_path)):
        logger.error("There was an error catching available query options.")
        return

    logger.info(f'{"-" * (55 + 15 + 25)}')
    logger.info(_format_settings_header([("name", 55), ("type", 15), ("value", 25)]))
    logger.info(f'{"-" * (55 + 15 + 25)}')
    for setting_name, setting_val in available_options.items():
        if default_query_options.get(setting_name) is not None:
            logger.info(
                f"{setting_name:<55}{setting_val['type']:<15}{default_query_options[setting_name]:<25}"
            )


def _available_query_options(profile: ProfileUserContext, resource_path: str) -> dict:
    response = basic_options(profile, resource_path, action="PUT")
    settings = response.get("settings", {})
    children = settings.get("children", {})
    return children.get("default_query_options", {}).get("children")


def _format_settings_header(headers_and_spacing: List[Tuple[str, int]]) -> str:
    format_strings = []
    for key, spacing in headers_and_spacing:
        format_strings.append(f"{key:<{spacing}}")
    return "".join(format_strings)


query_option.add_command(set_)
query_option.add_command(list_)
query_option.add_command(unset)
