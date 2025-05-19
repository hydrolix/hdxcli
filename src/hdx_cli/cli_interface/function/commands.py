"""Commands relative to function handling operations"""

import click

from ...library_api.common.exceptions import LogicException
from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.migration.resource_migrations import migrate_resource_config
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create

logger = get_logger()


@click.group(help="Function-related operations")
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--function",
    "function_name",
    help="Perform operation on the passed function.",
    metavar="FUNCTIONNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def function(ctx: click.Context, project_name: str, function_name: str):
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


@click.command(help="Create sql function.")
@click.option(
    "--sql-from-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
    help="Create the body of the sql from a json description as in the POST request in "
    "https://docs.hydrolix.io/docs/custom-functions."
    """For example:
              '{
                "sql": "(x, k, b) -> k*x + b;",
                "name": "linear_equation"
              }'"""
    ". 'name' will be replaced by FUNCTION_NAME",
    default=None,
)
@click.option("--inline-sql", "-s", help="Use inline sql in the command-line", default=None)
@click.argument("function_name", metavar="FUNCTION_NAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, function_name: str, sql_from_file: dict, inline_sql: str):
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
    if inline_sql:
        body["sql"] = inline_sql
    else:
        body = sql_from_file
    basic_create(profile, resource_path, function_name, body=body)
    logger.info(f"Created function {function_name}")


@click.command(
    help=(
        "Migrate a function to a target project and profile.\n\n"
        "This command migrates a function from the current source profile to the specified "
        "target project and target profile. The target profile can be provided directly using "
        "the --target-profile option, or by specifying the target cluster details such as "
        "hostname, username, password, and URI scheme."
    )
)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME", required=True, default=None)
@click.argument("new_function_name", metavar="FUNCTION_NAME", required=True, default=None)
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
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.functionname:
        raise click.BadParameter("No source function provided.")
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
