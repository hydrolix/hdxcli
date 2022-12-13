"""Transform resource command. It flows down url for current table"""
import click

from ..common.undecorated_click_commands import basic_transform
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.exceptions import (HdxCliException,
                                              TransformNotFoundException)

from ..common.rest_operations import (create as command_create,
                                      delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ...library_api.common.context import ProfileUserContext
from ..common.misc_operations import settings as command_settings
from ..common.undecorated_click_commands import basic_create_with_body_from_string


@click.group(help="Transform-related operations")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def transform(ctx: click.Context):
    basic_transform(ctx)


@click.command(help='Create transform.')
@click.option('--body-from-file', '-f',
              help='Use file contents as the transform settings.'
              "'name' key from the body will be replaced by the given 'resource_name'.",
              metavar='BODYFROMFILE',
              default=None)
@click.argument('transform_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           transform_name: str,
           body_from_file):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    with open(body_from_file, "r", encoding="utf-8") as f:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           transform_name, f.read())
    print(f'Created transform {transform_name}.')


transform.add_command(create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
