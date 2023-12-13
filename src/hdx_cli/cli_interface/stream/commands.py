import click

from ...library_api.common.exceptions import ResourceNotFoundException
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ..common.cached_operations import find_transforms
from ...library_api.common.context import ProfileUserContext


def _get_content_type(obj_type):
    content_types = {
        'csv': 'text/csv',
        'json': 'application/json'
    }
    return content_types.get(obj_type, 'text/csv')


@click.group(help="Stream-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--transform', 'transform_name',
              help="Explicitly pass the transform name. If none is given, "
                   "the default transform for the used table is used.",
              metavar='TRANSFORMNAME', default=None)
@click.pass_context
def stream(ctx: click.Context,
           project_name,
           table_name,
           transform_name):
    user_profile = ctx.parent.obj['usercontext']
    stream_path = '/ingest/event'
    ctx.obj = {'resource_path': stream_path,
               'usercontext': user_profile}
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name,
                                      transformname=transform_name)


@click.command(help='Ingest data via stream.')
@click.argument('stream_data_file')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def ingest(ctx: click.Context,
           stream_data_file: str):
    resource_path = ctx.parent.obj['resource_path']
    user_profile = ctx.parent.obj['usercontext']
    if not user_profile.projectname or not user_profile.tablename:
        raise ResourceNotFoundException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{user_profile.profilename}'")

    transform_name = user_profile.transformname
    transforms_list = find_transforms(user_profile)
    try:
        if transform_name:
            transform_name, transform_type = [(t['name'], t['type']) for t in transforms_list
                                              if t['name'] == transform_name][0]
        else:
            transform_name, transform_type = [(t['name'], t['type']) for t in transforms_list
                                              if t['settings']['is_default']][0]
    except IndexError as exc:
        raise ResourceNotFoundException('No default transform found to apply ingest command and '
                                        'no --transform passed') from exc

    with open(stream_data_file, 'rb') as data_file:
        data = data_file.read()

    hostname = user_profile.hostname
    scheme = user_profile.scheme
    timeout = user_profile.timeout
    url = f'{scheme}://{hostname}{resource_path}'
    token = user_profile.auth
    headers = {
        'Authorization': f'{token.token_type} {token.token}',
        'content-type': _get_content_type(transform_type),
        'x-hdx-table': f'{user_profile.projectname}.{user_profile.tablename}',
        'x-hdx-transform': transform_name
    }
    rest_ops.create(url, body=data,
                    body_type=bytes,
                    headers=headers,
                    timeout=timeout)
    print('Created stream ingest')


stream.add_command(ingest)
