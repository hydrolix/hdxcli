import click
import json

from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.context import ProfileUserContext
from ..common.rest_operations import delete as command_delete
from ..common.undecorated_click_commands import basic_settings
from ..common.undecorated_click_commands import basic_create_with_body_from_string
from ..common.rest_operations import (list_ as command_list,
                                      show as command_show)


@click.group(help="Pool-related operations")
@click.option('--pool', 'pool_name', help="Perform operation on the passed pool.",
              metavar='POOLNAME', default=None)
@click.pass_context
def pool(ctx: click.Context,
         pool_name):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile,
                                      poolname=pool_name)
    ctx.obj = {'resource_path': f'/config/v1/pools/',
               'usercontext': user_profile}


def build_request_body(replicas, cpu, memory, storage, pool_service):
    return {
        'description': 'Created with hdxcli tool',
        'settings': {
            'k8s_deployment': {
                'replicas': str(replicas),
                'cpu': str(cpu),
                'memory': f'{memory}Gi',
                'storage': f'{storage}Gi',
                'service': pool_service
            }
        }
    }


@click.command(help='Create new workload isolation resources')
@click.option('--replicas', '-r',
              type=int,
              help='Number of replicas for the workload (default: 1)',
              required=False,
              default=1)
@click.option('--cpu', '-c',
              type=float,
              help='Dedicated CPU allocation for each replica (default: 0.5)',
              required=False,
              default=0.5)
@click.option('--memory', '-m',
              type=float,
              help='Dedicated memory allocation for each replica, expressed in Gi (default: 0.5)',
              required=False,
              default=0.5)
@click.option('--storage', '-s',
              type=float,
              help='Storage capacity for each replica, expressed in Gi (default: 0.5)',
              required=False,
              default=0.5)
@click.argument('pool_service')
@click.argument('pool_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           replicas: int,
           cpu: float,
           memory: int,
           storage: int,
           pool_service: str,
           pool_name: str):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')

    body = build_request_body(replicas, cpu, memory, storage, pool_service)
    basic_create_with_body_from_string(user_profile, resource_path, pool_name, json.dumps(body))
    print(f'Created pool {pool_name}')


@click.command(help="Get, set or list settings on a resource. When invoked with "
               "only the key, it retrieves the value of the setting. If retrieved "
               "with both key and value, the value for the key, if it exists, will "
               "be set.\n"
               "Otherwise, when invoked with no arguments, all the settings will be listed.")
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def settings(ctx: click.Context,
             key,
             value):
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    the_value = value
    if value:
        the_value = value
        if (stripped := value.strip()).startswith('[') and stripped.endswith(']'):
            the_value = json.loads(stripped)
    basic_settings(profile, resource_path, key, the_value)


pool.add_command(command_list)
pool.add_command(create)
pool.add_command(command_delete)
pool.add_command(command_show)
pool.add_command(settings)
