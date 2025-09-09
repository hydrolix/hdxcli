import click

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    report_error_and_exit,
    ensure_logged_in,
)
from .alter.commands import alter as alter_command
from .batch.commands import batch as batch_command
from ..common.click_extensions import HdxGroup, HdxCommand

logger = get_logger()


@click.group(cls=HdxGroup)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def job(ctx: click.Context):
    """Manage batch and alter jobs."""
    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    jobs_path = f"/config/v1/orgs/{org_id}/jobs/"
    ctx.obj = {"resource_path": jobs_path, "usercontext": user_profile}


@click.command(cls=HdxCommand)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    help="Skip confirmation prompt.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def purgejobs(ctx: click.Context, yes: bool):
    """Purge all batch jobs in the organization."""
    if not yes:
        click.confirm(
            "Are you sure you want to purge all jobs?",
            abort=True
        )

    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    purgejobs_path = f"/config/v1/orgs/{org_id}/purgejobs/"
    basic_create(user_profile, purgejobs_path)
    logger.info("All completed and failed jobs have been purged.")


job.add_command(alter_command)
job.add_command(batch_command)
job.add_command(purgejobs)
