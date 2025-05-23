import json
from urllib.parse import urlparse

import click

from ...library_api.common.exceptions import LogicException
from ...library_api.utility.decorators import (
    confirmation_prompt,
    ensure_logged_in,
    report_error_and_exit,
    skip_group_logic_on_help,
)
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.cached_operations import find_tables, find_transforms
from ..common.undecorated_click_commands import basic_create, basic_delete, basic_show, basic_update


@click.group(
    help="Shadow table related operations. Shadow tables allow safe testing "
    "of transform changes by re-ingesting a small data sample."
)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile. Just for the shadow table creation.",
    metavar="TABLENAME",
    default=None,
)
@click.option(
    "--transform",
    "transform_name",
    help="Explicitly pass the transform name to be used as the source for the shadow table. Just "
    "for the shadow table creation.",
    metavar="TRANSFORMNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def shadow(ctx: click.Context, project_name: str, table_name: str, transform_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, transformname=transform_name
    )

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


@click.command(
    help="Create a shadow table. Requires --project, --table, and --transform "
    "options to specify the source of the shadow table."
)
@click.argument(
    "transform_settings",
    type=click.Path(exists=True, readable=True),
    metavar="TRANSFORM_SETTINGS",
    required=True,
    callback=load_json_settings_file,
)
@click.option(
    "--sample-rate",
    type=click.IntRange(0, 5),
    required=True,
    default=None,
    help="Percentage of the original data to be ingested in the shadow table.",
)
@click.option(
    "--table-name",
    type=str,
    required=False,
    default=None,
    help="Name of the shadow table. Default: shadow + source table name.",
)
@click.option(
    "--table-settings",
    type=click.Path(exists=True, readable=True),
    required=False,
    default=None,
    callback=load_json_settings_file,
    help="Path to a file containing settings for the shadow table.",
)
@click.option(
    "--transform-name",
    type=str,
    required=False,
    default=None,
    help="Name of the transform for the shadow table. Default: shadow + source transform name.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_shadow_table(
    ctx: click.Context,
    sample_rate: int,
    transform_settings: dict,
    table_name: str,
    table_settings: dict,
    transform_name: str,
):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    source_table_name = user_profile.tablename
    source_transform_name = user_profile.transformname

    if not source_table_name:
        raise click.BadParameter(
            "No source table name provided. Use --table option after table command."
        )
    if not source_transform_name:
        raise click.BadParameter(
            "No source transform name provided. Use --transform option after table command."
        )

    source_table_id = json.loads(basic_show(user_profile, resource_path, source_table_name)).get(
        "uuid"
    )
    source_transform = json.loads(
        basic_show(
            user_profile, f"{resource_path}{source_table_id}/transforms/", source_transform_name
        )
    )
    source_transform_path = (
        f'{resource_path}{source_table_id}/transforms/{source_transform.get("uuid")}/'
    )

    # Create shadow table name if not provided
    if not table_name:
        table_name = f"shadow_{source_table_name}"
    # Create transform name for the shadow table if not provided
    if not transform_name:
        transform_name = f"shadow_{source_transform_name}"

    # Settings file could be passed with or without the "settings" key
    if table_settings and not table_settings.get("settings"):
        table_settings = {"settings": table_settings}

    shadow_table = basic_create(user_profile, resource_path, table_name, body=table_settings).json()
    shadow_table_id = shadow_table.get("uuid")
    click.echo(f"Created shadow table {table_name}")

    # Create the transform that belongs to the shadow table
    resource_path = f"{resource_path}{shadow_table_id}/transforms/"
    shadow_transform = basic_create(
        user_profile, resource_path, transform_name, body=transform_settings
    ).json()
    shadow_transform_id = shadow_transform.get("uuid")
    click.echo(f"Created transform {transform_name}")

    # Set shadow configuration into the source transform
    source_transform["settings"]["shadow_table"] = {
        "table_id": shadow_table_id,
        "transform_id": shadow_transform_id,
        "rate": sample_rate / 100,
    }
    basic_update(user_profile, source_transform_path, body=source_transform)
    click.echo("Set shadow configuration")
    click.echo(f"Shadow table created successfully")


@click.command(
    help="Delete shadow table. Removes the shadow table settings "
    "from the source transform and deletes the table."
)
@click.argument(
    "table_name",
    type=str,
    metavar="TABLENAME",
    required=True,
)
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    help="Suppress confirmation to delete resource.",
    show_default=True,
    default=False,
)
@confirmation_prompt(
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete_shadow_table(ctx: click.Context, table_name: str, disable_confirmation_prompt: bool):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, table_name
    )
    try:
        del source_transform["settings"]["shadow_table"]
    except KeyError:
        raise LogicException("Source transform has no shadow table settings.")

    # Remove the shadow table settings from the source transform
    _update_source_transform(user_profile, source_transform)

    # Delete the shadow table
    if not basic_delete(user_profile, resource_path, table_name):
        raise LogicException(f"There was an error deleting the shadow table '{table_name}'")
    click.echo(f"Deleted {table_name}")


@click.command(
    help="Start sampling. Sets the specified sampling rate "
    "on the source transform for the shadow table."
)
@click.argument(
    "table_name",
    type=str,
    metavar="TABLENAME",
    required=True,
)
@click.option(
    "--sample-rate",
    type=click.IntRange(1, 5),
    required=True,
    default=None,
    help="Percentage of the original data to be ingested in the shadow table.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def start_sampling_shadow_table(ctx: click.Context, table_name: str, sample_rate: int):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, table_name
    )

    # Start sampling
    source_transform["settings"]["shadow_table"]["rate"] = sample_rate / 100
    _update_source_transform(user_profile, source_transform)
    click.echo(f"Started sampling of shadow table {table_name}")


@click.command(
    help="Stop sampling. Sets the sampling rate for "
    "the shadow table on the source transform to 0."
)
@click.argument(
    "table_name",
    type=str,
    metavar="TABLENAME",
    required=True,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def stop_sampling_shadow_table(ctx: click.Context, table_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Get the source transform of the shadow table
    source_transform = _get_source_transform_of_shadow_table(
        user_profile, resource_path, table_name
    )

    # Check if the shadow table is already stopped
    if source_transform["settings"]["shadow_table"].get("rate") == 0:
        click.echo(f"Shadow table '{table_name}' is already stopped")
        return

    # Stop sampling
    source_transform["settings"]["shadow_table"]["rate"] = 0
    _update_source_transform(user_profile, source_transform)
    click.echo(f"Stopped sampling of shadow table {table_name}")


def _get_source_transform_of_shadow_table(
    user_profile: ProfileUserContext, resource_path: str, table_name: str
) -> dict:
    """
    Get the source transform of the shadow table name.

    Args:
        user_profile: User profile context.
        resource_path: Resource path for the project.
        table_name: Name of the shadow table.

    Returns:
        dict: The source transform of the shadow table.

    Raises:
        ResourceNotFoundException: If the shadow table does not exist.
        LogicException: If the shadow table is not a shadow table.
    """
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
    """
    Update the source transform of the shadow table. It takes the transform path
    from the URL.
    """
    url = transform.get("url")
    update_resource_path = urlparse(url).path
    basic_update(user_profile, update_resource_path, body=transform)


shadow.add_command(create_shadow_table, name="create")
shadow.add_command(delete_shadow_table, name="delete")
shadow.add_command(start_sampling_shadow_table, name="start")
shadow.add_command(stop_sampling_shadow_table, name="stop")
