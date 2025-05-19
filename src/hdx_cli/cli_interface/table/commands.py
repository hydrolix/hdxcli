"""Commands relative to tables handling operations"""

import json

import click

from ...library_api.common.exceptions import HttpException, LogicException
from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from ...library_api.utility.file_handling import load_json_settings_file, load_plain_file
from ...models import ProfileUserContext
from ..common.migration.resource_migrations import migrate_resource_config
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import activity as command_activity
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.rest_operations import stats as command_stats
from ..common.undecorated_click_commands import basic_create, basic_show

logger = get_logger()


@click.group(help="Table-related operations")
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
    help="Use or override table set in the profile",
    metavar="TABLENAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def table(ctx: click.Context, project_name: str, table_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    ProfileUserContext.update_context(user_profile, projectname=project_name, tablename=table_name)
    project_name = user_profile.projectname
    if not project_name:
        raise LogicException(
            f"No project parameter provided and "
            f"no project is set in profile '{user_profile.profilename}'"
        )

    project_body = access_resource(user_profile, [("projects", project_name)])
    project_id = project_body.get("uuid")
    org_id = user_profile.org_id
    ctx.obj = {
        "resource_path": f"/config/v1/orgs/{org_id}/projects/{project_id}/tables/",
        "usercontext": user_profile,
    }


@click.command(help="Create table.")
@click.argument("table_name", metavar="TABLENAME", required=True)
@click.option(
    "--type",
    "-t",
    "table_type",
    type=click.Choice(("turbine", "summary")),
    required=False,
    default="turbine",
    help="Create a regular table or an aggregation table (summary). Default: turbine",
)
@click.option(
    "--sql-query",
    "-s",
    type=str,
    required=False,
    default=None,
    help="SQL query to use (for summary tables only)",
)
@click.option(
    "--sql-query-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    required=False,
    default=None,
    callback=load_plain_file,
    help="File path to SQL query to use (for summary tables only)",
)
@click.option(
    "--settings-file",
    "-S",
    type=click.Path(exists=True, readable=True),
    required=False,
    default=None,
    callback=load_json_settings_file,
    help="Path to a file containing settings for the table",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    table_name: str,
    table_type: str,
    sql_query: str,
    sql_query_file: str,
    settings_file: dict,
):
    if table_type == "summary" and not (
        (sql_query and not sql_query_file) or (sql_query_file and not sql_query)
    ):
        raise click.MissingParameter(
            "When creating a summary table, either SQL query or SQL query file must be provided."
        )

    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    body = {}
    if settings_file:
        body.update(settings_file)
    body["name"]: table_name

    if table_type == "summary":
        summary_sql_query = sql_query_file if sql_query_file else sql_query
        body["type"] = "summary"

        settings = body.get("settings", {})
        summary_settings = settings.get("summary", {})
        summary_settings["sql"] = summary_sql_query
        settings["summary"] = summary_settings
        body["settings"] = settings

    basic_create(user_profile, resource_path, table_name, body=body)
    logger.info(f"Created table {table_name}")


def _truncate_table(profile: ProfileUserContext, resource_path: str, table_name: str):
    table_ = json.loads(basic_show(profile, resource_path, table_name))
    table_id = table_.get("uuid")
    truncate_url = f"{resource_path}{table_id}/truncate"
    try:
        basic_create(profile, truncate_url)
        logger.info(f"Truncated table {table_name}")
    except HttpException as exc:
        logger.debug(f"Error truncating table {table_name}: {exc}")
        logger.info(f"Could not truncate table {table_name}")


@click.command(help="Truncate table.")
@click.argument("table_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def command_truncate(ctx: click.Context, table_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _truncate_table(user_profile, resource_path, table_name)


@click.command(
    help=(
        "Migrate a table and its associated transforms.\n\n"
        "This command migrates a table from the source project in the current profile to a "
        "target project and profile. By default, the associated transforms within the table "
        "are also migrated.\n\n"
        "Options allow you to customize the migration:\n"
        "- Use --only (-O) to migrate only the table, skipping all associated transforms.\n\n"
        "Provide a target profile using --target-profile or specify cluster details "
        "(hostname, username, password, and URI scheme)."
    )
)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME", required=True, default=None)
@click.argument("new_table_name", metavar="NEW_TABLE_NAME", required=True, default=None)
@target_cluster_options
@no_rollback_option
@click.option(
    "-O",
    "--only",
    required=False,
    default=False,
    is_flag=True,
    help="Migrate only the table, skipping its associated transforms.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    target_project_name: str,
    new_table_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
    only: bool,
):
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.tablename:
        raise click.BadParameter("No source table name provided.")
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
        "target_table": new_table_name,
        "no_rollback": no_rollback,
        "only": only,
    }
    migrate_resource_config("table", **data)

    logger.info("All resources migrated successfully")


table.add_command(create)
table.add_command(command_delete)
table.add_command(command_list)
table.add_command(command_show)
table.add_command(command_settings)
table.add_command(command_truncate, name="truncate")
table.add_command(command_activity)
table.add_command(command_stats)
table.add_command(migrate)
