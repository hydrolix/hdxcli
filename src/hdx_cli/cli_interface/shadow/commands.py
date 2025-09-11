import json
from functools import partial
from urllib.parse import urlparse

import click

from hdx_cli.cli_interface.common.cached_operations import find_tables, find_transforms
from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create,
    basic_delete,
    basic_show,
    basic_update,
)
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    dynamic_confirmation_prompt,
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECT_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def shadow(ctx: click.Context, project_name: str):
    """Shadow tables allow safe testing of transform changes by
    re-ingesting a small data sample from a source table."""
    user_profile = ctx.parent.obj.get("usercontext")
    ProfileUserContext.update_context(user_profile, projectname=project_name)

    project_name = user_profile.projectname
    if not project_name:
        raise LogicException(
            f"No project parameter provided and "
            f"no project is set in profile '{user_profile.profilename}'"
        )

    org_id = user_profile.org_id
    resource_path = f"/config/v1/orgs/{org_id}/projects/"
    project_body = json.loads(basic_show(user_profile, resource_path, project_name))
    project_id = project_body.get("uuid")
    ctx.obj = {
        "resource_path": f"{resource_path}{project_id}/tables/",
        "usercontext": user_profile,
    }


@click.command(cls=HdxCommand)
@click.argument(
    "transform_settings_path",
    type=click.Path(exists=True, readable=True),
    required=True,
    metavar="TRANSFORM_SETTINGS_PATH",
)
@click.option(
    "--source-table",
    required=True,
    help="The source table to shadow.",
)
@click.option(
    "--source-transform",
    required=True,
    help="The source transform to shadow.",
)
@click.option(
    "--sample-rate",
    type=click.IntRange(0, 5),
    required=True,
    help="Percentage of the original data to be ingested in the shadow table.",
)
@click.option(
    "--table-name",
    "shadow_table_name",
    required=False,
    default=None,
    help="Name of the shadow table. Default: shadow_ + 'source-table-name'.",
)
@click.option(
    "--table-settings",
    "table_settings_path",
    type=click.Path(exists=True, readable=True),
    required=False,
    default=None,
    help="Path to a file containing settings for the shadow table.",
)
@click.option(
    "--transform-name",
    "shadow_transform_name",
    required=False,
    default=None,
    help="Name of the transform for the shadow table. Default: shadow_ + 'source-transform-name'.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    transform_settings_path: str,
    source_table: str,
    source_transform: str,
    sample_rate: int,
    shadow_table_name: str,
    table_settings_path: str,
    shadow_transform_name: str,
):
    """Create a new shadow table. This command creates a new shadow table
    and a corresponding transform based on a source table and transform.
    It requires the source context to be specified via options.

    \b
    Examples:
      # Create a shadow table with a 2% sample rate
      {full_command_prefix} create ./new_transform_for_shadow.json --source-table my_table --source-transform my_transform --sample-rate 2
    """
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Load settings from files
    transform_settings = read_json_from_file(transform_settings_path)
    table_settings = read_json_from_file(table_settings_path) if table_settings_path else None

    # Set table context to fetch source transform
    user_profile.tablename = source_table
    source_table_id = json.loads(basic_show(user_profile, resource_path, source_table)).get("uuid")
    source_transform_obj = json.loads(
        basic_show(user_profile, f"{resource_path}{source_table_id}/transforms/", source_transform)
    )
    source_transform_path = (
        f'{resource_path}{source_table_id}/transforms/{source_transform_obj.get("uuid")}/'
    )

    # Set names if not provided
    if not shadow_table_name:
        shadow_table_name = f"shadow_{source_table}"
    if not shadow_transform_name:
        shadow_transform_name = f"shadow_{source_transform}"

    # Settings file could be passed with or without the "settings" key
    if table_settings and not table_settings.get("settings"):
        table_settings = {"settings": table_settings}

    shadow_table = basic_create(
        user_profile, resource_path, shadow_table_name, body=table_settings
    ).json()
    shadow_table_id = shadow_table.get("uuid")
    logger.info(f"Created shadow table '{shadow_table_name}'")

    # Create the transform that belongs to the shadow table
    resource_path = f"{resource_path}{shadow_table_id}/transforms/"
    shadow_transform = basic_create(
        user_profile, resource_path, shadow_transform_name, body=transform_settings
    ).json()
    shadow_transform_id = shadow_transform.get("uuid")
    logger.info(f"Created transform '{shadow_transform_name}'")

    # Set shadow configuration into the source transform
    source_transform_obj["settings"]["shadow_table"] = {
        "table_id": shadow_table_id,
        "transform_id": shadow_transform_id,
        "rate": sample_rate / 100,
    }
    basic_update(user_profile, source_transform_path, body=source_transform_obj)
    logger.info("Updated source transform with shadow configuration")
    logger.info(f"Shadow table '{shadow_table_name}' created successfully")


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    help="Suppress confirmation to delete the {resource}.",
    show_default=True,
    default=False,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt: bool):
    """Delete a {resource} table.

    This command removes the {resource} table settings from the source transform
    and then deletes the {resource} table itself.

    \b
    Examples:
      # Delete the {resource} table named 'my_shadow_table'
      {full_command_prefix} delete my_shadow_table
    """
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, resource_name
    )
    try:
        del source_transform["settings"]["shadow_table"]
    except KeyError:
        raise LogicException("Source transform has no shadow table settings.")

    # Remove the shadow table settings from the source transform
    _update_source_transform(user_profile, source_transform)

    # Delete the shadow table
    if not basic_delete(user_profile, resource_path, resource_name):
        raise LogicException(f"There was an error deleting the shadow table '{resource_name}'")
    logger.info(f"Deleted shadow table {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--sample-rate",
    type=click.IntRange(1, 5),
    required=True,
    help="Percentage of the original data to be ingested in the shadow table.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def start(ctx: click.Context, resource_name: str, sample_rate: int):
    """Start or update sampling for a shadow table, setting the
    specified sampling rate on the source transform.

    \b
    Examples:
      # Start sampling 3% of data for 'my_shadow_table'
      {full_command_prefix} start my_shadow_table --sample-rate 3
    """
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, resource_name
    )

    # Start sampling
    source_transform["settings"]["shadow_table"]["rate"] = sample_rate / 100
    _update_source_transform(user_profile, source_transform)
    logger.info(f"Started sampling for shadow table '{resource_name}' at {sample_rate}%")


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def stop(ctx: click.Context, resource_name: str):
    """Stop sampling for a shadow table, setting the
    sampling rate on the source transform to 0.

    \b
    Examples:
      # Stop all sampling for 'my_shadow_table'
      {full_command_prefix} stop my_shadow_table
    """
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, resource_name
    )

    # Check if the shadow table is already stopped
    if source_transform["settings"]["shadow_table"].get("rate") == 0:
        logger.info(f"Shadow table '{resource_name}' is already stopped")
        return

    # Stop sampling
    source_transform["settings"]["shadow_table"]["rate"] = 0
    _update_source_transform(user_profile, source_transform)
    logger.info(f"Stopped sampling for shadow table '{resource_name}'")


def _get_source_transform_of_shadow_table(
    user_profile: ProfileUserContext, resource_path: str, table_name: str
) -> dict:
    """Get the source transform of the shadow table name."""
    shadow_table_uuid = json.loads(basic_show(user_profile, resource_path, table_name)).get("uuid")

    tables = find_tables(user_profile)
    table_names = [table["name"] for table in tables if table["name"] != table_name]

    for name in table_names:
        user_profile.tablename = name
        transforms = find_transforms(user_profile)

        if not transforms:
            continue

        for transform in transforms:
            shadow_table_settings = transform.get("settings", {}).get("shadow_table", {})
            if shadow_table_settings and shadow_table_settings.get("table_id") == shadow_table_uuid:
                return transform

    raise LogicException(f"'{table_name}' is not a shadow table.")


def _update_source_transform(user_profile: ProfileUserContext, transform: dict) -> None:
    """Update the source transform of the shadow table. It takes
    the transform path from the URL."""
    url = transform.get("url")
    update_resource_path = urlparse(url).path
    basic_update(user_profile, update_resource_path, body=transform)


shadow.add_command(create)
shadow.add_command(delete)
shadow.add_command(start)
shadow.add_command(stop)
