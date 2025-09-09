import json

import click
from rich.console import Console
from rich.table import Table

from hdx_cli.cli_interface.common.cached_operations import find_alter_jobs
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_show, basic_list
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.utility.functions import heuristically_get_resource_kind
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
@click.option(
    "--job",
    "alter_name",
    default=None,
    help="Perform an operation on the passed job name.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def alter(ctx: click.Context, alter_name: str):
    """Manage alter jobs."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, altername=alter_name)
    alter_path = f'{ctx.parent.obj["resource_path"]}alter/'
    ctx.obj = {"resource_path": alter_path, "usercontext": user_profile}


@alter.group(cls=HdxGroup)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context):
    """Create a new alter job."""
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = "/query"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@create.command(cls=HdxCommand, name="update")  # Type: ignore
@click.option(
    "--table",
    required=True,
    help="The table to alter, e.g., my_proj.my_tbl.",
)
@click.option("--column", required=True, help="The column to update.")
@click.option("--value", required=True, help="The new value for the column.")
@click.option("--where", required=True, help="The WHERE clause for the update operation.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_update(ctx: click.Context, table: str, column: str, value, where: str):
    """Create a job to update specific rows in a table.

    \b
    Examples:
      # Update the 'status' column to 'hidden' for specific rows
      {full_command_prefix} update --table my_proj.my_tbl --column status --value hidden --where "timestamp < '2020-10-10'"
    """
    update_query = f"ALTER TABLE {table} UPDATE {column} = '{value}' WHERE {where}"
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, body=update_query, body_type="csv")
    logger.info("Created 'UPDATE' alter job")


@create.command(cls=HdxCommand, name="delete")
@click.option(
    "--table", required=True, help="The table to alter, e.g., my_proj.my_tbl."
)
@click.option("--where", required=True, help="The WHERE clause for the delete operation.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_delete(ctx: click.Context, table: str, where: str):
    """Create a job to delete specific rows from a table.

    \b
    Examples:
      # Delete all rows older than a specific date
      {full_command_prefix} delete --table my_proj.my_tbl --where "timestamp < '2024-01-01'"
    """
    delete_query = f"ALTER TABLE {table} DELETE WHERE {where}"
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, body=delete_query, body_type="csv")
    logger.info("Created 'DELETE' alter job")


@alter.command(cls=HdxCommand, name="list")
@click.option("--status", "status", default=None, help="Filter alter jobs by status.")
@click.option("--project", "project_name", default=None, help="Filter alter jobs by project name.")
@click.option("--table", "table_name", default=None, help="Filter alter jobs by table name.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, status: str, project_name: str, table_name: str):
    """List all alter jobs.

    \b
    Examples:
      # List all alter jobs with the status 'done'
      {full_command_prefix} list --status done
    """
    profile = ctx.parent.obj["usercontext"]
    list_alter_jobs(profile, status, project_name, table_name)


@alter.command(cls=HdxCommand)
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def commit(ctx: click.Context, job_name: str):
    """Commit changes made by an alter job.

    \b
    Examples:
      # Commit the changes for the alter job named 'alter_job_123'
      {full_command_prefix} commit alter_job_123
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "commit")
    logger.info(f"Committing job '{job_name}'")


@alter.command(cls=HdxCommand)
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context, job_name: str):
    """Cancel an ongoing alter job.

    \b
    Examples:
      # Cancel the alter job named 'alter_job_123'
      {full_command_prefix} cancel alter_job_123
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "cancel")
    logger.info(f"Cancelled job '{job_name}'")


@alter.command(cls=HdxCommand)
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx: click.Context, job_name: str):
    """Retry a failed alter job.

    \b
    Examples:
      # Retry the failed job named 'alter_job_123'
      {full_command_prefix} retry alter_job_123
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "retry")
    logger.info(f"Retrying job '{job_name}'")


@alter.command(cls=HdxCommand)
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def verify(ctx: click.Context, job_name: str):
    """Verify the status of an alter job.

    \b
    Examples:
      # Verify the status of the job named 'alter_job_123'
      {full_command_prefix} verify alter_job_123
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    if not job_name:
        _, resource_kind = heuristically_get_resource_kind(resource_path)
        if not (job_name := getattr(profile, resource_kind + "name")):
            raise LogicException(f"No default {resource_kind} found in profile.")

    job_id = json.loads(basic_show(profile, resource_path, job_name)).get("uuid")
    verify_alter_path = f"{resource_path}{job_id}/verify"
    basic_list(profile, verify_alter_path)


def perform_alter_job(profile: ProfileUserContext, resource_path: str, job_name: str, action: str):
    job_id = json.loads(basic_show(profile, resource_path, job_name)).get("uuid")
    resource_action_path = f"{resource_path}{job_id}/{action}"
    basic_create(profile, resource_action_path)


def list_alter_jobs(
    profile: ProfileUserContext,
    status_to_filter: str,
    project_to_filter: str,
    table_to_filter: str,
) -> None:
    alter_job_list = find_alter_jobs(profile)
    if status_to_filter is not None:
        alter_job_list = filter(lambda x: x.get("status") == status_to_filter, alter_job_list)
    if project_to_filter is not None:
        alter_job_list = filter(
            lambda x: x.get("settings", {}).get("project_name") == project_to_filter,
            alter_job_list,
        )
    if table_to_filter is not None:
        alter_job_list = filter(
            lambda x: x.get("settings", {}).get("table_name") == table_to_filter,
            alter_job_list,
        )

    filtered_and_reduced_data = list(
        map(
            lambda x: (
                x.get("name"),
                f'{x.get("settings", {}).get("project_name")}.'
                f'{x.get("settings", {}).get("table_name")}',
                x.get("status"),
            ),
            alter_job_list,
        )
    )

    if not filtered_and_reduced_data:
        return

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name")
    table.add_column("Table")
    table.add_column("Status")

    for job in filtered_and_reduced_data:
        table.add_row(job[0], job[1], job[2])

    console.print(table)


alter.add_command(command_delete)
alter.add_command(command_show)
