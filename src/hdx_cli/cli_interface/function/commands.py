import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.library_api.common.generic_resource import access_resource
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
    "--function",
    "function_name",
    help="Perform operation on the passed function.",
    metavar="FUNCTION_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def function(ctx: click.Context, project_name: str, function_name: str):
    """This group of commands allows creating, listing, showing, deleting,
    and migrating functions. A project context is required for all
    operations."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, functionname=function_name
    )
    project_name = user_profile.projectname

    if not project_name:
        raise LogicException(
            f"No project parameter provided and "
            f"no project set in profile '{user_profile.profilename}'"
        )

    project_body = access_resource(user_profile, [("projects", project_name)])
    project_id = project_body.get("uuid")
    org_id = user_profile.org_id
    ctx.obj = {
        "resource_path": f"/config/v1/orgs/{org_id}/projects/{project_id}/functions/",
        "usercontext": user_profile,
    }


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--sql-from-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    default=None,
    help="Path to a JSON file with the function definition.",
)
@click.option("--inline-sql", "-s", help="Use inline sql in the command-line", default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, resource_name: str, sql_from_file: str, inline_sql: str):
    """A {resource} can be created either from an inline SQL string
    or from a JSON file containing the {resource} definition.

    \b
    Examples:
      # Create a {resource} from an inline SQL string
      {full_command_prefix} create {example_name} --inline-sql "(url) -> domain(url)"

    \b
      # Create a {resource} from a JSON file
      {full_command_prefix} create {example_name} --sql-from-file path/to/func.json
    """
    if inline_sql and sql_from_file:
        raise LogicException(
            "Only one of the options --inline-sql and --sql-from-file can be used."
        )
    if not inline_sql and not sql_from_file:
        raise LogicException(
            "You need at least one of --inline-sql or --sql-from-file to create a function."
        )

    resource_path = ctx.parent.obj["resource_path"]
    profile = ctx.parent.obj["usercontext"]
    body = {}
    if sql_from_file:
        body = read_json_from_file(sql_from_file)
    else:
        body["sql"] = inline_sql

    basic_create(profile, resource_path, resource_name, body=body)
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME")
@click.argument("new_function_name", metavar="NEW_FUNCTION_NAME")
@target_cluster_options
@no_rollback_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    target_project_name: str,
    new_function_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
):
    """Migrate a {resource} to a different project.

    Migrates a {resource} from a source context (in the current profile)
    to a target project, which can be in the same or a different cluster.
    Authentication for the target cluster can be provided via a separate profile
    using `--target-profile` or by specifying credentials directly.

    By default, any failure during the process will trigger a rollback of the
    changes made. Use the `--no-rollback` flag to disable this behavior.

    \b
    Examples:
      # Migrate '{example_name}' to a new project 'my_target_project'
      {full_command_prefix} --{resource} {example_name} migrate my_target_project my_new_{resource}
    """
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.functionname:
        raise click.BadParameter(
            "A source function must be specified with the --function option.",
            param_hint="--function",
        )

    has_target_profile = target_profile is not None
    has_all_cluster_options = all(
        [
            target_cluster_hostname,
            target_cluster_username,
            target_cluster_password,
            target_cluster_uri_scheme,
        ]
    )
    if not has_target_profile and not has_all_cluster_options:
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
        "source_function": source_profile.functionname,
        "target_function": new_function_name,
        "no_rollback": no_rollback,
    }
    migrate_resource_config("function", **data)

    logger.info("All resources migrated successfully")


function.add_command(create)
function.add_command(command_delete)
function.add_command(command_list)
function.add_command(command_show)
function.add_command(command_settings)
function.add_command(migrate)
