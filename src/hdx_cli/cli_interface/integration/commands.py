import json
import click

from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.context import ProfileUserContext
from ...library_api.utility.decorators import report_error_and_exit
from ..common.undecorated_click_commands import basic_create_with_body_from_string


from ..common.undecorated_click_commands import basic_transform

_REPO_USER = 'hydrolix/transforms'


@click.group(help="Public resources management")
@click.pass_context
def integration(ctx: click.Context):
    profile: ProfileUserContext = ctx.parent.obj['usercontext']

    ctx.obj = {'resource_path': f'/{_REPO_USER}/',
               'cluster_hostname': profile.hostname,
               'usercontext': profile}
    profile.hostname = 'raw.githubusercontent.com'


@click.group(help="Public transforms.")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.pass_context
def transform(ctx: click.Context,
              project_name,
              table_name):
    user_profile: ProfileUserContext = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name)
    resource_path = ctx.parent.obj['resource_path']
    resource_path = f'{resource_path}dev/'
    ctx.obj = {'resource_path': resource_path,
               'cluster_hostname': ctx.parent.obj['cluster_hostname'],
               'usercontext': user_profile}


integration.add_command(transform)


def _github_list(ctx: click.Context):
    profile: ProfileUserContext = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    resource_path = f'{resource_path}index.json'
    url = f'{profile.scheme}://{profile.hostname}{resource_path}'
    timeout = profile.timeout
    return rest_ops.list(url, headers={}, timeout=timeout)


@click.command(help="Integration transforms.")
@click.pass_context
def list_(ctx: click.Context):
    results = _github_list(ctx)
    for obj in results:
        name = obj['name']
        description = obj['description']
        vendor = obj['vendor']
        print(f'{name: <20} {description: <70} from {vendor: <40}')


transform.add_command(list_, name='list')


def _basic_show(ctx: click.Context,
                transform_name):
    results = _github_list(ctx)
    base_resource_url = 'https://raw.githubusercontent.com/hydrolix/transforms/dev'
    for res in results:
        if res['name'] == transform_name:
            resource_url = f'{base_resource_url}{res["url"]}'
            user_profile = ctx.parent.obj['usercontext']
            timeout = user_profile.timeout
            result = rest_ops.list(resource_url, headers={}, timeout=timeout)
            return json.dumps(result)
    else:
        raise ValueError(f'No transform named {transform_name}')


@click.command(help="Apply an integration transform into current table.")
@click.argument('integration_transform_name', metavar='INTEGRATIONTRANSFORMNAME')
@click.argument('transform_name', metavar='TRANSFORMNAME')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def apply(ctx: click.Context,
          integration_transform_name,
          transform_name):
    transform_contents = _basic_show(ctx, integration_transform_name)
    user_profile = ctx.parent.obj['usercontext']
    user_profile.hostname = ctx.parent.obj['cluster_hostname']

    basic_transform(ctx)
    resource_path = ctx.obj['resource_path']
    basic_create_with_body_from_string(user_profile,
                                       resource_path,
                                       transform_name,
                                       transform_contents)
    print(f'Created transform {transform_name} from {integration_transform_name}.')


@click.command(help="Integration transforms.")
@click.argument('transform_name', metavar='TRANSFORMNAME')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, transform_name):
    #results = _github_list(ctx)
    #base_resource_url = 'https://raw.githubusercontent.com/hydrolix/transforms/dev'
    # 'Fastly/fastly_transform.json'
    print(_basic_show(ctx, transform_name))


transform.add_command(apply, 'apply')
transform.add_command(show)
