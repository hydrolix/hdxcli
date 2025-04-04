import json
from typing import Any

import click

from ...library_api.utility.decorators import force_operation_option, report_error_and_exit
from ..common.undecorated_click_commands import basic_settings


@click.command(
    help="Get, set or list settings on a resource. When invoked with "
    "only the key, it retrieves the value of the setting. If retrieved "
    "with both key and value, the value for the key, if it exists, will "
    "be set.\n"
    "Otherwise, when invoked with no arguments, all the settings will be listed."
)
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def settings(ctx: click.Context, key: str, value: str):
    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    the_value = value_formatter(value)
    basic_settings(profile, resource_path, key, the_value)


@click.command(
    help="Get, set or list settings on a resource. When invoked with "
    "only the key, it retrieves the value of the setting. If retrieved "
    "with both key and value, the value for the key, if it exists, will "
    "be set.\n"
    "Otherwise, when invoked with no arguments, all the settings will be listed."
)
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@force_operation_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def settings_with_force(ctx: click.Context, key: str, value: str, force_operation: bool):
    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    the_value = value_formatter(value)
    basic_settings(profile, resource_path, key, the_value, force_operation=force_operation)


def value_formatter(value: str) -> Any:
    if not value:
        return value

    the_value = value
    if (stripped := value.strip()).startswith("[") and stripped.endswith("]"):
        try:
            the_value = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise click.BadParameter(
                "The provided list value is in an incorrect format. "
                "Please ensure the list is properly quoted, e.g., '[\"str\", int]'."
            ) from exc
    return the_value
