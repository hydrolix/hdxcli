import click

from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    confirmation_prompt,
    ensure_logged_in,
    report_error_and_exit,
)
from ..common.undecorated_click_commands import basic_create
from .alter.commands import alter as alter_command
from .batch.commands import batch as batch_command

logger = get_logger()


@click.group(help="Job-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def job(ctx: click.Context):
    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    jobs_path = f"/config/v1/orgs/{org_id}/jobs/"
    ctx.obj = {"resource_path": jobs_path, "usercontext": user_profile}


@click.command(help="Purge all batch jobs in your org.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@confirmation_prompt(
    prompt="Please type 'purge all jobs' to proceed: ",
    confirmation_message="purge all jobs",
    fail_message="Incorrect prompt input: jobs have not been purged",
)
def purgejobs(ctx: click.Context):
    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    purgejobs_path = f"/config/v1/orgs/{org_id}/purgejobs/"
    basic_create(user_profile, purgejobs_path)
    logger.info("All jobs purged")


job.add_command(alter_command)
job.add_command(batch_command)
job.add_command(purgejobs)
