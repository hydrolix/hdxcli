import json
from functools import partial

import click

from ...common.undecorated_click_commands import DEFAULT_INDENTATION, get_resource_list
from ....library_api.common.context import ProfileUserContext
from ....library_api.common.exceptions import ResourceNotFoundException, LogicException
from ....library_api.common.logging import get_logger
from ....library_api.common import rest_operations as rest_ops
from ....library_api.userdata.token import AuthInfo
from ....library_api.utility.decorators import report_error_and_exit, dynamic_confirmation_prompt
from ....library_api.utility.functions import heuristically_get_resource_kind

logger = get_logger()


@click.group(help="Alter Job-related operations")
@click.option('--project', 'project_name', metavar='PROJECTNAME', default=None,
              help="Use or override project set in the profile.")
@click.option('--table', 'table_name', metavar='TABLENAME', default=None,
              help="Use or override table set in the profile.")
@click.option('--job', 'alter_name', metavar='JOBNAME', default=None,
              help="Perform operation on the passed job name.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def alter(ctx: click.Context, project_name: str, table_name: str, alter_name: str):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(
        user_profile,
        projectname=project_name,
        tablename=table_name,
        altername=alter_name
    )
    alter_path = ctx.parent.obj['resource_path'] + 'alter/'
    ctx.obj = {'resource_path': alter_path,
               'usercontext': user_profile}


@alter.group(help="Create a new alter job.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = '/query'
    ctx.obj = {'resource_path': resource_path,
               'usercontext': user_profile}


@create.command(help='Create a job to update specific rows in a table.', name='update')  # Type: ignore
@click.option('--table', required=True, help='The table to alter, e.g., sample_project.sample_table.')
@click.option('--column', required=True, help='The column to update.')
@click.option('--value', required=True, help='The new value for the column.')
@click.option('--where', required=True, help='The WHERE clause for the update operation.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_update(ctx: click.Context, table: str, column: str, value, where: str):
    update_query = f"ALTER TABLE {table} UPDATE {column} = '{value}' WHERE {where}"
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    _create_alter_job(user_profile, resource_path, update_query)
    logger.info('Created UPDATE alter job')


@create.command(help='Create a job to delete specific rows from a table.', name='delete')  # Type: ignore
@click.option('--table', required=True, help='The table to alter, e.g., sample_project.sample_table.')
@click.option('--where', required=True, help='The WHERE clause for the delete operation.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_delete(ctx: click.Context, table: str, where: str):
    delete_query = f"ALTER TABLE {table} DELETE WHERE {where}"
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    _create_alter_job(user_profile, resource_path, delete_query)
    logger.info('Created DELETE alter job')


@alter.command(help='List all alter jobs.', name='list')
@click.option('--status', 'status', default=None, help='Filter alter jobs by status.')
@click.option('--project', 'project_name', default=None, help='Filter alter jobs by project name.')
@click.option('--table', 'table_name', default=None, help='Filter alter jobs by table name.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, status: str, project_name: str, table_name: str):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    list_alter_jobs(profile, resource_path, status, project_name, table_name)


@alter.command(help='Display details of a specific alter job.')
@click.argument('job_name', required=False)
@click.option('-i', '--indent', is_flag=True, default=False, help='Indent the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, job_name: str, indent: bool,):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    job = _get_alter_job(profile, resource_path, job_name)
    indentation = DEFAULT_INDENTATION if indent else None
    logger.info(json.dumps(job, indent=indentation))


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message='delete this resource',
    fail_message='Incorrect prompt input: resource was not deleted'
)


@alter.command(help='Delete an existing alter job.')
@click.option('--disable-confirmation-prompt', is_flag=True, show_default=True, default=False,
              help='Suppress confirmation to delete resource.')
@click.argument('resource_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt: bool):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get('resource_path')
    profile = ctx.parent.obj.get('usercontext')
    if delete_alter(profile, resource_path, resource_name):
        logger.info(f'Deleted {resource_name}')
    else:
        logger.info(f'Could not delete {resource_name}. Not found')


@alter.command(help='Commit changes made by an alter job.')
@click.argument('job_name', required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def commit(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    perform_alter_job(profile, resource_path, job_name, 'commit')
    logger.info(f'Committing {job_name}')


@alter.command(help='Cancel an ongoing alter job.')
@click.argument('job_name', required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    perform_alter_job(profile, resource_path, job_name, 'cancel')
    logger.info(f'Cancelled {job_name}')


@alter.command(help='Retry a failed alter job.')
@click.argument('job_name', required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx: click.Context, job_name: str):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    perform_alter_job(profile, resource_path, job_name, 'retry')
    logger.info(f'Retrying {job_name}')


@alter.command(help='Verify the status of an alter job.')
@click.argument('job_name', required=False)
@click.option('-i', '--indent', is_flag=True, default=False, help='Indent the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def verify(ctx: click.Context, job_name: str, indent: bool):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    if verify_result := verify_alter(profile, resource_path, job_name, indent):
        logger.info(f'{verify_result}')


def delete_alter(profile: ProfileUserContext, resource_path: str, job_name: str) -> bool:
    job_id = _get_alter_job(profile, resource_path, job_name).get('uuid')

    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}{job_id}'
    auth_info: AuthInfo = profile.auth
    timeout = profile.timeout
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    rest_ops.delete(url, headers=headers, timeout=timeout)
    return True


def perform_alter_job(profile: ProfileUserContext, resource_path: str, job_name: str, action: str):
    job_id = _get_alter_job(profile, resource_path, job_name).get('uuid')

    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}{job_id}/{action}'
    auth_info: AuthInfo = profile.auth
    timeout = profile.timeout
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    rest_ops.create(url, headers=headers, timeout=timeout)


def verify_alter(profile: ProfileUserContext,
                 resource_path: str,
                 job_name: str,
                 indentation: bool = False
                 ) -> str:
    job_id = _get_alter_job(profile, resource_path, job_name).get('uuid')

    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}{job_id}/verify/'
    auth_info: AuthInfo = profile.auth
    timeout = profile.timeout
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    results = rest_ops.get(url, headers=headers, timeout=timeout).get('results')
    if results:
        indentation = DEFAULT_INDENTATION if indentation else None
        return json.dumps(results[0], indent=indentation)


def _create_alter_job(profile: ProfileUserContext, resource_path: str, alter_job) -> None:
    scheme = profile.scheme
    hostname = profile.hostname
    url = f'{scheme}://{hostname}{resource_path}'
    timeout = profile.timeout
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    rest_ops.create(url, headers=headers, timeout=timeout, body=alter_job, body_type='csv')


def _get_alter_job(profile: ProfileUserContext, resource_path: str, job_name: str) -> dict:
    if not job_name:
        _, resource_kind = heuristically_get_resource_kind(resource_path)
        if not (job_name := getattr(profile, resource_kind + 'name')):
            raise LogicException(f'No default {resource_kind} found in profile.')

    alter_jobs = get_resource_list(profile, resource_path).get('results')
    for job in alter_jobs:
        if job.get('name') == job_name:
            return job

    raise ResourceNotFoundException('Cannot find resource.')


def list_alter_jobs(profile: ProfileUserContext,
                    resource_path: str,
                    status_to_filter: str,
                    project_to_filter: str,
                    table_to_filter: str
                    ) -> None:
    default_alter_job_list = get_resource_list(profile, resource_path).get('results')

    if status_to_filter is not None:
        default_alter_job_list = filter(
            lambda x: x.get('status') == status_to_filter, default_alter_job_list)
    if project_to_filter is not None:
        default_alter_job_list = filter(
            lambda x: x.get('settings', {}).get('project_name') == project_to_filter, default_alter_job_list)
    if table_to_filter is not None:
        default_alter_job_list = filter(
            lambda x: x.get('settings', {}).get('table_name') == table_to_filter, default_alter_job_list)

    filtered_and_reduced_data = list(
        map(lambda x: (x.get('name'),
                       f'{x.get("settings", {}).get("project_name")}.'
                       f'{x.get("settings", {}).get("table_name")}',
                       x.get('status')),
            default_alter_job_list)
    )

    if not filtered_and_reduced_data:
        return

    logger.info(f'{"-" * (20 + 40 + 15)}')
    logger.info(f'{"name":20}'
                f'{"table":40}'
                f'{"status":15}')
    logger.info(f'{"-" * (20 + 40 + 15)}')
    for alter_job in filtered_and_reduced_data:
        logger.info(f"{alter_job[0]:<20}"
                    f"{alter_job[1]:<40}"
                    f"{alter_job[2]:<15}")
