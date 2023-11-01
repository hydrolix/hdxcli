import click

from ...library_api.common.exceptions import LogicException, ResourceNotFoundException
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit
from ..common.cached_operations import find_projects, find_tables, find_transforms


@click.group(help="Stream-related operations")
@click.pass_context
def stream(ctx):
    profileinfo = ctx.parent.obj['usercontext']
    stream_path = f'/ingest/event'
    ctx.obj = {'resource_path': stream_path,
               'usercontext': profileinfo}


@click.command(help='Ingest data via stream.')
@click.argument('stream_data_file')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def ingest(ctx: click.Context,
           stream_data_file: str):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    if not profile.projectname or not profile.tablename:
        raise ResourceNotFoundException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{profile.profilename}'")

    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {
        'Authorization': f'{token.token_type} {token.token}',
        'content-type': 'text/csv'}

    transformname = profile.transformname
    if not transformname:
        transforms_list = find_transforms(profile)
        try:
            transformname = [t['name'] for t in transforms_list if t['settings']['is_default']][0]
        except IndexError as exc:
            raise ResourceNotFoundException('No default transform found to apply ingest command and '
                                            'no --transform passed') from exc

    headers['x-hdx-table'] = f'{profile.projectname}.{profile.tablename}'
    headers['x-hdx-transform'] = transformname
    with open(stream_data_file, 'rb') as data_file:
        data = data_file.read()

    rest_ops.create(url, body=data, body_type=bytes, headers=headers)
    print(f'Created stream ingest')


stream.add_command(ingest)