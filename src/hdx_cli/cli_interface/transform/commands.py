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


@click.group(help="Transform-related operations")
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def transform(ctx: click.Context):
    basic_transform(ctx)


transform.add_command(command_create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
#transform.add_command(command_set_default)
