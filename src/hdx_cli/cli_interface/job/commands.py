from typing import Tuple
import json

import click


from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.context import ProfileUserContext
from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)
from ...library_api.utility.decorators import (report_error_and_exit,
                                               confirmation_prompt)
from ..common.misc_operations import settings as command_settings
from ..common.cached_operations import find_transforms


@click.group(help="Job-related operations")
@click.pass_context
def job(ctx: click.Context):
    user_profile = ctx.parent.obj['usercontext']
    org_id = user_profile.org_id
    jobs_path = f'/config/v1/orgs/{org_id}/jobs/'
    ctx.obj = {'resource_path': jobs_path,
               'usercontext': user_profile}


@click.command(help='Purge all batch jobs in your org')
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
    print('All jobs purged')


@click.group(help="Job-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--transform', 'transform_name',
              help="Explicitly pass the transform name. If none is given, "
                   "the default transform for the used table is used.",
              metavar='TRANSFORMNAME', default=None)
@click.option('--job', 'batch_name', help="Perform operation on the passed job name.",
              metavar='JOBNAME', default=None)
@click.pass_context
def batch(ctx: click.Context,
          project_name,
          table_name,
          transform_name,
          batch_name):
    user_profile = ctx.parent.obj['usercontext']
    batch_path = ctx.parent.obj['resource_path'] + 'batch/'
    ctx.obj = {'resource_path': batch_path,
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name,
                                      transformname=transform_name,
                                      batchname=batch_name)


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
               ' (and not your client machine executing hdxcli tool) *must have* permissions to access '
               'the data bucket in case you need to ingest directly from there. If the data path is a '
               'directory, the directory data will be used. ')
@click.argument('jobname')
@click.argument('jobname_file')
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint:enable=line-too-long
def ingest(ctx: click.Context,
           jobname: str,
           jobname_file: str):
    resource_path = ctx.parent.obj['resource_path']
    user_profile = ctx.parent.obj['usercontext']
    if not user_profile.projectname or not user_profile.tablename:
        raise ResourceNotFoundException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{user_profile.profilename}'")

    hostname = user_profile.hostname
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    url = f'{scheme}://{hostname}{resource_path}'
    token = user_profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    with open(jobname_file, 'r', encoding='utf-8') as job_input:
        body = json.load(job_input)
        body['name'] = jobname
        body['settings']['source']['table'] = f'{user_profile.projectname}.{user_profile.tablename}'

        transformname = user_profile.transformname
        if not transformname:
            transforms_list = find_transforms(user_profile)
            try:
                transformname = [t['name'] for t in transforms_list if t['settings']['is_default']][0]
            except IndexError as exc:
                raise LogicException('No default transform found to apply ingest command and '
                                     'no --transform passed') from exc
        body['settings']['source']['transform'] = transformname
        rest_ops.create(url, body=body, headers=headers, timeout=timeout)
    print(f'Started job {jobname}')


@click.command(help='Cancels a job.')
@click.argument('job_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context,
           job_name):
    resource_path = ctx.parent.obj['resource_path']
    user_profile = ctx.parent.obj['usercontext']
    hostname = user_profile.hostname
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = user_profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url,
                              headers=headers,
                              timeout=timeout)
    job_id = None
    for a_resource in resources:
        if a_resource['name'] == job_name:
            job_id = a_resource['uuid']
            break
    if not job_id:
        print(f'Could not cancel {ctx.parent.command.name} {job_name}. Not found.')
    else:
        cancel_job_url = f'{list_url}{job_id}/cancel'
        rest_ops.create(cancel_job_url, headers=headers, timeout=timeout)
        print(f'Cancelled {job_name}')


@click.command(help='Retries a job.')
@click.argument('job_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx,
          job_name):
    resource_path = ctx.parent.obj['resource_path']
    user_profile = ctx.parent.obj['usercontext']
    hostname = user_profile.hostname
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = user_profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers, timeout=timeout)
    job_id = None
    for a_resource in resources:
        if a_resource['name'] == job_name:
            job_id = a_resource['uuid']
            break
    if not job_id:
        print(f'Could not retry {ctx.parent.command.name} {job_name}. Not found.')
    else:
        retry_job_url = f'{list_url}{job_id}/retry'
        rest_ops.create(retry_job_url, headers=headers, timeout=timeout)
        print(f'Retrying {job_name}')


command_ingest = ingest
command_cancel = cancel
command_retry = retry

batch.add_command(command_ingest)
batch.add_command(command_cancel)
batch.add_command(command_retry)
