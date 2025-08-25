import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_transform
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

from .compare import compare

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECT_NAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLE_NAME",
    default=None,
)
@click.option(
    "--transform",
    "transform_name",
    help="Explicitly pass the transform name. If none is given, "
    "the default transform for the table is used.",
    metavar="TRANSFORM_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def transform(ctx: click.Context, project_name: str, table_name: str, transform_name: str):
    """This group of commands allows to create, list, show, delete, and
    migrate transforms. A project and table context is required for
    all operations."""
    # The 'compare' command handles its own resource loading (local files or cluster)
    if ctx.invoked_subcommand == "compare":
        return

    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, transformname=transform_name
    )
    basic_transform(ctx)


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--body-from-file",
    "-f",
    "settings_filename",
    type=click.Path(exists=True, readable=True),
    help="Path to a JSON file with transform settings.",
    required=True,
)
@click.option(
    "-F",
    "--force",
    is_flag=True,
    default=False,
    help='This flag allows adding the "force_operation" parameter to the request.',
    hidden=True,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, resource_name: str, settings_filename: str, force: bool):
    """Creates a new {resource} from a JSON configuration file
    in the specified project and table.

    \b
    Examples:
      # Create a new {resource} from a JSON configuration file
      {full_command_prefix} create {example_name} --body-from-file path/to/your-transform.json
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    settings_body = read_json_from_file(settings_filename)
    basic_create(
        user_profile,
        resource_path,
        resource_name,
        body=settings_body,
        force_operation=force,
    )
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME")
@click.argument("target_table_name", metavar="TARGET_TABLE_NAME")
@click.argument("new_transform_name", metavar="NEW_TRANSFORM_NAME")
@target_cluster_options
@no_rollback_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    target_project_name: str,
    target_table_name: str,
    new_transform_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
):
    """Migrate a {resource} to a different project and table.

    Migrates a {resource} from a source context (in the current profile)
    to a target table, which can be in the same or a different cluster.
    Authentication for the target cluster can be provided via a separate profile
    using `--target-profile` or by specifying credentials directly.

    By default, any failure during the process will trigger a rollback of the
    changes made. Use the `--no-rollback` flag to disable this behavior.

    \b
    Examples:
      # Migrate the {resource} '{example_name}' to a new project and table in a different cluster
      {full_command_prefix} --{resource} {example_name} migrate my_target_project my_target_table my_new_{resource} --target-profile prod-us-west
    """
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.transformname:
        raise click.BadParameter(
            "A source transform must be specified using the --transform option.",
            param_hint="--transform",
        )
    if target_profile is None and not (
        target_cluster_hostname
        and target_cluster_username
        and target_cluster_password
        and target_cluster_uri_scheme
    ):
        raise click.BadParameter(
            "Either provide a --target-profile or all four target cluster options."
        )

    data = {
        "source_profile": source_profile,
        "target_profile_name": target_profile,
        "target_cluster_hostname": target_cluster_hostname,
        "target_cluster_username": target_cluster_username,
        "target_cluster_password": target_cluster_password,
        "target_cluster_uri_scheme": target_cluster_uri_scheme,
        "source_project": source_profile.projectname,
        "target_project": target_project_name,
        "source_table": source_profile.tablename,
        "target_table": target_table_name,
        "source_transform": source_profile.transformname,
        "target_transform": new_transform_name,
        "no_rollback": no_rollback,
    }
    migrate_resource_config("transform", **data)

    logger.info("All resources migrated successfully")


transform.add_command(create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
transform.add_command(migrate)
transform.add_command(compare)
