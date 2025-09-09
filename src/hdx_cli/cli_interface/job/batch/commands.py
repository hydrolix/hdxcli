import json

import click
from rich.console import Console
from rich.table import Table

from hdx_cli.cli_interface.common.cached_operations import find_transforms, find_batch_jobs
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_show
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.library_api.common.exceptions import ResourceNotFoundException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
@click.option(
    "--job",
    "batch_name",
    metavar="JOB_NAME",
    default=None,
    help="Perform operation on the passed job name.",
)
@click.pass_context
def batch(ctx: click.Context, batch_name: str):
    """Manage batch jobs."""
    user_profile = ctx.parent.obj["usercontext"]
    batch_path = f'{ctx.parent.obj["resource_path"]}batch/'
    ctx.obj = {"resource_path": batch_path, "usercontext": user_profile}
    ProfileUserContext.update_context(
        user_profile,
        batchname=batch_name,
    )


@batch.command(cls=HdxCommand)
@click.argument("job_name")
@click.argument(
    "settings_file_path",
    type=click.Path(exists=True, readable=True)
)
@click.option(
    "--project",
    "project_name",
    help="Override the project for the ingest job."
)
@click.option(
    "--table",
    "table_name",
    help="Override the table for the ingest job."
)
@click.option(
    "--transform",
    "transform_name",
    help="Override the transform to use. Defaults to the table's default transform.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def ingest(
    ctx: click.Context,
    job_name: str,
    settings_file_path: str,
    project_name: str,
    table_name: str,
    transform_name: str
):
    """Create an ingest job from a settings file.

    This command creates a batch ingest job based on a JSON configuration file.
    The file defines the data source (e.g., a cloud storage bucket), and other
    job-specific settings.

    You can override the destination project, table, and transform specified
    within the settings file by using the `--project`, `--table`, and `--transform`
    options.

    \b
    Examples:
      # Create an ingest job from a file, overriding the destination table
      {full_command_prefix} ingest my-batch-job ./aws-s3-settings.json --project my_proj --table my_tabl
    """
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    body = read_json_from_file(settings_file_path)

    # Override project and table if provided
    if project_name and table_name:
        body["settings"]["source"]["table"] = f"{project_name}.{table_name}"
    elif project_name or table_name:
        raise click.BadOptionUsage("project", "Both --project and --table must be provided together.")

    # Determine the transform to use
    final_project, final_table = body["settings"]["source"]["table"].split('.')
    ProfileUserContext.update_context(user_profile, projectname=final_project, tablename=final_table)

    if not transform_name:
        if not (transform_name := body["settings"]["source"].get("transform")):
            transforms_list = find_transforms(user_profile)
            try:
                transform_name = [t["name"] for t in transforms_list if t["settings"]["is_default"]][0]
            except (IndexError, KeyError) as exc:
                raise ResourceNotFoundException(
                    "No default transform found for the table and no --transform was provided."
                ) from exc

    body["settings"]["source"]["transform"] = transform_name
    basic_create(user_profile, resource_path, job_name, body=body)
    logger.info(f"Started job '{job_name}'")


@batch.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    """List all batch jobs.

    \b
    Examples:
      # List all batch jobs in the organization
      {full_command_prefix} list
    """
    profile = ctx.parent.obj["usercontext"]
    job_list = find_batch_jobs(profile)

    if not job_list:
        return

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name")
    table.add_column("Table")
    table.add_column("Status")

    for job in job_list:
        job_name = job.get("name")
        job_status = job.get("status")
        job_table = job.get("settings", {}).get("source", {}).get("table", "N/A")
        table.add_row(job_name, job_table, job_status)

    console.print(table)


@batch.command(cls=HdxCommand)
@click.argument("job_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context, job_name: str):
    """Cancel a running batch job.

    \b
    Examples:
      # Cancel the job named 'batch-job-123'
      {full_command_prefix} cancel batch-job-123
    """
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    batch_job_id = json.loads(basic_show(user_profile, resource_path, job_name)).get("uuid")
    cancel_job_path = f"{resource_path}{batch_job_id}/cancel"
    basic_create(user_profile, cancel_job_path)
    logger.info(f"Cancelled batch job '{job_name}'")


@batch.command(cls=HdxCommand)
@click.argument("job_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx, job_name: str):
    """Retry a failed batch job.

    \b
    Examples:
      # Retry the failed job named 'batch-job-123'
      {full_command_prefix} retry batch-job-123
    """
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    batch_job_id = json.loads(basic_show(user_profile, resource_path, job_name)).get("uuid")
    retry_job_path = f"{resource_path}{batch_job_id}/retry"
    basic_create(user_profile, retry_job_path)
    logger.info(f"Retried batch job '{job_name}'")


batch.add_command(command_delete)
batch.add_command(command_show)
batch.add_command(command_settings)
