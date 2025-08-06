import json
from typing import Any

import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_settings
from hdx_cli.library_api.utility.decorators import report_error_and_exit


def _value_formatter(value: str) -> Any:
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


@click.command(cls=HdxCommand, name="settings")
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@click.option(
    "-F",
    "--force",
    is_flag=True,
    default=False,
    help='This flag allows adding the `force_operation` parameter to the request.',
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def settings(ctx: click.Context, key: str | None, value: str | None, force: bool):
    """List, get, or set key-value settings for a specific {resource}.

    \b
    This command operates in three modes:
    - *LIST*: Invoked with no arguments, it lists all settings.
    - *GET*: Invoked with only a KEY, it retrieves the value of that setting.
    - *SET*: Invoked with a KEY and a VALUE, it sets the value for that setting.

    The VALUE can be a string, a number, or a JSON-formatted string for lists/objects.
    When setting a value, the `--force-operation` option may be required for certain resource.

    \b
    Examples:
      # List all settings for the {resource} '{example_name}'
      {full_command_prefix} --{resource} {example_name} settings

    \b
      # Get the 'name' setting for the {resource} '{example_name}'
      {full_command_prefix} --{resource} {example_name} settings name

    \b
      # Set a new 'name' setting for the {resource} '{example_name}'
      {full_command_prefix} --{resource} {example_name} settings name new_name
    """
    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    formatted_value = _value_formatter(value)
    basic_settings(profile, resource_path, key, formatted_value, force_operation=force)
