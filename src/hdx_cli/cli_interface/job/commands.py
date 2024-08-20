import click

from .alter.commands import alter as alter_command
from .batch.commands import batch as batch_command

from ...library_api.utility.decorators import report_error_and_exit, confirmation_prompt, ensure_logged_in
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.logging import get_logger


logger = get_logger()


@click.group(help="Job-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def job(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    jobs_path = f'/config/v1/orgs/{org_id}/jobs/'
    ctx.obj = {'resource_path': jobs_path,
               'usercontext': user_profile}


@click.command(help='Purge all batch jobs in your org.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@confirmation_prompt(prompt="Please type 'purge all jobs' to proceed: ",
                     confirmation_message='purge all jobs',
                     fail_message='Incorrect prompt input: jobs have not been purged')
def purgejobs(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    purgejobs_path = f'/config/v1/orgs/{org_id}/purgejobs/'
    hostname = user_profile.hostname
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    purgejobs_url = f'{scheme}://{hostname}{purgejobs_path}'

    auth = user_profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    rest_ops.create(purgejobs_url, headers=headers, timeout=timeout)
    logger.info('All jobs purged')


job.add_command(alter_command)
job.add_command(batch_command)
job.add_command(purgejobs)
