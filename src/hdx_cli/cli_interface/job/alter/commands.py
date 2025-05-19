import json

import click

from ....library_api.common.exceptions import LogicException
from ....library_api.common.logging import get_logger
from ....library_api.utility.decorators import report_error_and_exit
from ....library_api.utility.functions import heuristically_get_resource_kind
from ....models import ProfileUserContext
from ...common.cached_operations import find_alter_jobs
from ...common.rest_operations import delete as command_delete
from ...common.rest_operations import show as command_show
from ...common.undecorated_click_commands import (
    basic_create,
    basic_list,
    basic_show,
)

logger = get_logger()


@click.group(help="Alter Job-related operations")
@click.option(
    "--project",
    "project_name",
    metavar="PROJECTNAME",
    default=None,
    help="Use or override project set in the profile.",
)
@click.option(
    "--table",
    "table_name",
    metavar="TABLENAME",
    default=None,
    help="Use or override table set in the profile.",
)
@click.option(
    "--job",
    "alter_name",
    metavar="JOBNAME",
    default=None,
    help="Perform operation on the passed job name.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def alter(ctx: click.Context, project_name: str, table_name: str, alter_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, altername=alter_name
    )
    alter_path = f'{ctx.parent.obj["resource_path"]}alter/'
    ctx.obj = {"resource_path": alter_path, "usercontext": user_profile}


@alter.group(help="Create a new alter job.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = "/query"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@create.command(
    help="Create a job to update specific rows in a table.", name="update"
)  # Type: ignore
@click.option(
    "--table", required=True, help="The table to alter, e.g., sample_project.sample_table."
)
@click.option("--column", required=True, help="The column to update.")
@click.option("--value", required=True, help="The new value for the column.")
@click.option("--where", required=True, help="The WHERE clause for the update operation.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_update(ctx: click.Context, table: str, column: str, value, where: str):
    update_query = f"ALTER TABLE {table} UPDATE {column} = '{value}' WHERE {where}"
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, body=update_query, body_type="csv")
    logger.info("Created UPDATE alter job")


@create.command(
    help="Create a job to delete specific rows from a table.", name="delete"
)  # Type: ignore
@click.option(
    "--table", required=True, help="The table to alter, e.g., sample_project.sample_table."
)
@click.option("--where", required=True, help="The WHERE clause for the delete operation.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_delete(ctx: click.Context, table: str, where: str):
    delete_query = f"ALTER TABLE {table} DELETE WHERE {where}"
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, body=delete_query, body_type="csv")
    logger.info("Created DELETE alter job")


@alter.command(help="List all alter jobs.", name="list")
@click.option("--status", "status", default=None, help="Filter alter jobs by status.")
@click.option("--project", "project_name", default=None, help="Filter alter jobs by project name.")
@click.option("--table", "table_name", default=None, help="Filter alter jobs by table name.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, status: str, project_name: str, table_name: str):
    profile = ctx.parent.obj["usercontext"]
    list_alter_jobs(profile, status, project_name, table_name)


@alter.command(help="Commit changes made by an alter job.")
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def commit(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "commit")
    logger.info(f"Committing {job_name}")


@alter.command(help="Cancel an ongoing alter job.")
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "cancel")
    logger.info(f"Cancelled {job_name}")


@alter.command(help="Retry a failed alter job.")
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    perform_alter_job(profile, resource_path, job_name, "retry")
    logger.info(f"Retrying {job_name}")


@alter.command(help="Verify the status of an alter job.")
@click.argument("job_name", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def verify(ctx: click.Context, job_name: str):
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

    logger.info(f'{"-" * (20 + 40 + 15)}')
    logger.info(f'{"name":20}' f'{"table":40}' f'{"status":15}')
    logger.info(f'{"-" * (20 + 40 + 15)}')
    for alter_job in filtered_and_reduced_data:
        logger.info(f"{alter_job[0]:<20}" f"{alter_job[1]:<40}" f"{alter_job[2]:<15}")


alter.add_command(command_delete)
alter.add_command(command_show)
