import json

import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import activity as command_activity
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.rest_operations import stats as command_stats
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_show
from hdx_cli.library_api.common.exceptions import HttpException, LogicException
from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from hdx_cli.library_api.utility.file_handling import read_json_from_file, read_plain_file
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
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLE_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def table(ctx: click.Context, project_name: str, table_name: str):
    """This group of commands allows to create, list, show, delete, truncate,
    and migrate tables. A project context is required for all operations.
    """
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


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "--type",
    "-t",
    "table_type",
    type=click.Choice(("turbine", "summary"), case_sensitive=False),
    default="turbine",
    help="Specify the table type. Default: turbine.",
)
@click.option(
    "--sql-query",
    "-s",
    default=None,
    help="SQL query for 'summary' tables.",
)
@click.option(
    "--sql-query-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    default=None,
    help="Path to a file with the SQL query for 'summary' table.",
)
@click.option(
    "--settings-file",
    "-S",
    "settings_filename",
    type=click.Path(exists=True, readable=True),
    default=None,
    help="Path to a JSON file with additional table settings.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    table_type: str,
    sql_query: str,
    sql_query_file: str,
    settings_filename: str,
):
    """Create a new {resource}.

    Creates a standard (turbine) or a summary {resource}. For summary
    {resource_plural}, an SQL query must be provided via `--sql-query` or
    `--sql-query-file`.

    \b
    Examples:
      # Create a standard {resource} using a settings file
      {full_command_prefix} create {example_name} --settings-file path/to/settings.json

    \b
      # Create a summary {resource} using an SQL query from a file
      {full_command_prefix} create summary_table --type summary --sql-query-file path/to/query.sql
    """
    if table_type == "summary" and not (
        (sql_query and not sql_query_file) or (sql_query_file and not sql_query)
    ):
        raise click.MissingParameter(
            "When creating a summary table, either SQL query or SQL query file must be provided."
        )

    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Load settings from files if provided
    settings_body = read_json_from_file(settings_filename) if settings_filename else {}
    sql_query_from_file = read_plain_file(sql_query_file) if sql_query_file else None

    body = {}
    if settings_body:
        body.update(settings_body)

    if table_type == "summary":
        summary_sql_query = sql_query_from_file if sql_query_from_file else sql_query
        body["type"] = "summary"

        settings = body.get("settings", {})
        summary_settings = settings.get("summary", {})
        summary_settings["sql"] = summary_sql_query
        settings["summary"] = summary_settings
        body["settings"] = settings

    basic_create(user_profile, resource_path, resource_name, body=body)
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


def _truncate_table(profile: ProfileUserContext, resource_path: str, resource_name: str):
    table_ = json.loads(basic_show(profile, resource_path, resource_name))
    table_id = table_.get("uuid")
    truncate_url = f"{resource_path}{table_id}/truncate"
    try:
        basic_create(profile, truncate_url)
        logger.info(f"Truncated table {resource_name}")
    except HttpException as exc:
        logger.debug(f"Error truncating table {resource_name}: {exc}")
        logger.info(f"Could not truncate table {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "--yes",
    is_flag=True,
    help="Bypass the confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def truncate(ctx: click.Context, resource_name: str, yes: bool):
    """Remove all data from a {resource}.

    This action permanently deletes all rows from the {resource} but preserves
    the resource and its settings. This operation cannot be undone.

    \b
    Examples:
      # Truncate the {resource} named '{example_name}'
      {full_command_prefix} truncate {example_name}
    """
    if not yes:
        click.confirm(
            f"Are you sure you want to delete all rows from table '{resource_name}'? "
            "This action cannot be undone",
            abort=True,
        )

    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _truncate_table(user_profile, resource_path, resource_name)


@click.command(cls=HdxCommand)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME", required=True)
@click.argument("new_table_name", metavar="NEW_TABLE_NAME", required=True)
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
    """Migrate a {resource} and its transforms to a different project.

    Migrates a {resource} from a source context (current profile) to a target
    project, which can be in the same or a different cluster. All associated
    transforms are also migrated or use the `--only` flag to migrate only
    the {resource} and skip its transforms.
    Authentication for the target cluster can be provided via a separate profile
    using `--target-profile` or by specifying credentials directly.

    By default, any failure during the process will trigger a rollback of the
    changes made. Use the `--no-rollback` flag to disable this behavior.

    \b
    Examples:
      # Migrate '{example_name}' and its transforms to a new 'my_target_project' project
      {full_command_prefix} --{resource} {example_name} migrate my_target_project my_new_{resource}

      # Migrate only the {resource} '{example_name}' to a new 'my_target_project' project, without its transforms
      {full_command_prefix} --{resource} {example_name} migrate my_target_project my_new_{resource} --only
    """
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.tablename:
        raise click.BadParameter(
            "A source table must be specified with the --table option.",
            param_hint="--table",
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
table.add_command(truncate)
table.add_command(command_activity)
table.add_command(command_stats)
table.add_command(migrate)
