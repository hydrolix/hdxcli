from functools import partial
import click

from ...library_api.common.exceptions import LogicException
from ...library_api.common.logging import get_logger
from ...library_api.utility.functions import heuristically_get_resource_kind
from ...library_api.utility.decorators import (report_error_and_exit,
                                               dynamic_confirmation_prompt)
from .undecorated_click_commands import (basic_create,
                                         basic_delete,
                                         basic_list,
                                         basic_show,
                                         basic_activity,
                                         basic_stats)

logger = get_logger()


@click.command(help='Create resource.')
@click.option('--body-from-file', '-f',
              help='Create will use as body for request the file contents.'
              "'name' key from the body will be replaced by the given 'resource_name'.",
              default=None)
@click.option('--body-from-file', '-f',
              help='Create will use as body the file contents.',
              default=None)
@click.option('--body-from-file-type', '-t',
              type=click.Choice(('json', 'verbatim')),
              help='How to interpret the body from option. ',
              default='json')
@click.argument('resource_name')
# @click.option('--sql', '-s',
#               help="Create will use as 'sql' field the contents of the sql string",
#               default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           resource_name: str,
           body_from_file,
           body_from_file_type):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    basic_create(user_profile, resource_path,
                 resource_name, body_from_file, body_from_file_type)
    logger.info(f'Created {resource_name}')


_confirmation_prompt = partial(dynamic_confirmation_prompt,
                               prompt="Please type 'delete this resource' to delete: ",
                               confirmation_message='delete this resource',
                               fail_message='Incorrect prompt input: resource was not deleted')


@click.command(help='Delete resource.')
@click.option('--disable-confirmation-prompt',
              is_flag=True,
              help='Suppress confirmation to delete resource.', show_default=True, default=False)
@click.argument('resource_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str,
           disable_confirmation_prompt):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get('resource_path')
    profile = ctx.parent.obj.get('usercontext')
    if basic_delete(profile, resource_path, resource_name):
        logger.info(f'Deleted {resource_name}')
    else:
        logger.info(f'Could not delete {resource_name}. Not found')


@click.command(help='List resources.', name='list')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    resource_path = ctx.parent.obj.get('resource_path')
    profile = ctx.parent.obj.get('usercontext')
    basic_list(profile, resource_path)


@click.command(help='Show resource. If not resource_name is provided, it will show the default '
                    'if there is one.')
@click.option('-i', '--indent', is_flag=True, default=False,
              help='Indent the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    _, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + 'name')):
        raise LogicException(f'No default {resource_kind} found in profile')
    logger.info(basic_show(profile, resource_path,
                           resource_name, indent))


@click.command(help='Display the activity of a resource. If not resource_name is provided, '
                    'it will show the default if there is one.')
@click.option('-i', '--indent', is_flag=True, default=False,
              help='Indent the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def activity(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    _, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + 'name')):
        raise LogicException(f'No default {resource_kind} found in profile')
    logger.info(basic_activity(profile, resource_path, resource_name, indent))


@click.command(help='Display statistics for a resource. If not resource_name is provided, '
                    'it will show the default if there is one.')
@click.option('-i', '--indent', is_flag=True, default=False,
              help='Indent the output.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def stats(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    _, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + 'name')):
        raise LogicException(f'No default {resource_kind} found in profile')
    logger.info(basic_stats(profile, resource_path, resource_name, indent))
