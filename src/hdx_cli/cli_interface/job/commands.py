from typing import Tuple
import json

import click


from ...library_api.common.exceptions import LogicException, HdxCliException
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.auth import PROFILE_CONFIG_FILE
from ...library_api.common import rest_operations as rest_ops
from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)
from ...library_api.utility.decorators import (report_error_and_exit,
                                               confirmation_prompt)
from ..common.misc_operations import settings as command_settings
from ..common.cached_operations import find_projects, find_tables


@click.group(help="Job-related operations")
@click.pass_context
def job(ctx):
    profileinfo = ctx.parent.obj['usercontext']
    org_id = profileinfo.org_id
    jobs_path = f'/config/v1/orgs/{org_id}/jobs/'
    ctx.obj = {'resource_path': jobs_path,
               'usercontext': profileinfo}

@click.command(help='Purge all batch jobs in your org.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@confirmation_prompt(prompt="Please type 'purge all jobs' to proceed: ",
                     confirmation_message='purge all jobs',
                     fail_message='Incorrect prompt input: jobs have not been purged')
def purgejobs(ctx):
    profileinfo = ctx.parent.obj['usercontext']
    org_id = profileinfo.org_id
    purgejobs_path = f'/config/v1/orgs/{org_id}/purgejobs/'
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    scheme = profile.scheme
    purgejobs_url = f'{scheme}://{hostname}{purgejobs_path}'

    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    rest_ops.create(purgejobs_url, body=None, headers=headers)
    print('All jobs purged')

@click.group(help="Job-related operations")
@click.pass_context
def batch(ctx):
    profileinfo = ctx.parent.obj['usercontext']
    batch_path = ctx.parent.obj['resource_path'] + 'batch/'
    ctx.obj = {'resource_path': batch_path,
               'usercontext': profileinfo}


job.add_command(batch)
batch.add_command(command_delete)
batch.add_command(command_list)
batch.add_command(command_show)
batch.add_command(command_settings)


def _heuristically_get_resource_kind(resource_path) -> Tuple[str, str]:
    """Returns plural and singular names for resource kind given a resource path.
       If it is a nested resource
    For example:

          - /config/.../tables/ -> ('tables', 'table')
          - /config/.../projects/ -> ('projects', 'project')
          - /config/.../jobs/batch/ -> ('batch', 'batch')
    """
    split_path = resource_path.split("/")
    plural = split_path[-2]
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular

# pylint:disable=line-too-long
@click.command(help='Ingest data. The data path url can be local or point to a bucket. Your cluster '
                ' (and not your client machine executing hdx-cli tool) *must have* permissions to access '
                'the data bucket in case you need to ingest directly from there. If the data path is a '
                'directory, the directory data will be used. ')
@click.argument('jobname')
@click.argument('jobname_file')
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint:enable=line-too-long
def ingest(ctx: click.Context,
           jobname: str,
           jobname_file: str,
           ):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']

    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    with open(jobname_file, 'r', encoding='utf-8') as job_input:
        body = json.load(job_input)
        body['name'] = jobname
        body['settings']['source']['table'] = f'{profile.projectname}.{profile.tablename}'

        transformname = profile.transformname
        if not transformname:
            org_id = profile.org_id
            project_id = [p for p in find_projects(profile) if p['name'] == profile.projectname][0]['uuid']
            table_id = [t for t in find_tables(profile) if t['name'] == profile.tablename][0]['uuid']

            transforms_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/transforms/'
            transforms_url = f'{scheme}://{hostname}{transforms_path}'
            transforms_list = rest_ops.list(transforms_url,
                                            headers=headers)
            try:
                transformname = [t['name'] for t in transforms_list if t['settings']['is_default']][0]
            except IndexError as exc:
                raise LogicException('No default transform found to apply ingest command and no --transform-name passed') from exc
        body['settings']['source']['transform'] = transformname
        rest_ops.create(url, body=body, headers=headers)
    print(f'Started job {jobname}.')


@click.command(help='Cancels a job.')
@click.argument('job_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx,
           job_name):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    scheme = profile.scheme
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    job_id = None
    for a_resource in resources:
        if a_resource['name'] == job_name:
            job_id = a_resource['uuid']
            break
    if not job_id:
        print(f'Could not cancel {ctx.parent.command.name} {job_name}. Not found.')
    else:
        cancel_job_url = f'{list_url}{job_id}/cancel'
        print(cancel_job_url)
        rest_ops.create(cancel_job_url, headers=headers, body=None)
        print(f'Cancelled {job_name}')

@click.command(help='Retries a job.')
@click.argument('job_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx,
           job_name):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    hostname = profile.hostname
    scheme = profile.scheme
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    job_id = None
    for a_resource in resources:
        if a_resource['name'] == job_name:
            job_id = a_resource['uuid']
            break
    if not job_id:
        print(f'Could not retry {ctx.parent.command.name} {job_name}. Not found.')
    else:
        retry_job_url = f'{list_url}{job_id}/retry'
        print(retry_job_url)
        rest_ops.create(retry_job_url, headers=headers, body=None)
        print(f'Retrying {job_name}')


command_ingest = ingest
command_cancel = cancel
command_retry = retry

batch.add_command(command_ingest)
batch.add_command(command_cancel)
batch.add_command(command_retry)
