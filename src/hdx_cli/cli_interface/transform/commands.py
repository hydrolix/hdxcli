"""Transform resource command. It flows down url for current table"""
import json

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

from ...library_api.ddl.sql import (ddl_to_create_table_info, ddl_to_hdx_datatype,
                                    DdlCreateTableInfo,
                                    ColumnDefinition,
                                    generate_transform_dict)
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


@click.command(help='Map a transform from a description language. (currently sql CREATE TABLE)')
@click.option('--ddl-mapping',
              help='Mapping repository file to use for ddl to data type translation',
              metavar='DDLMAPPING',
              required=False)
@click.option('--ddl-custom-mapping',
              help='Mapping file to use for the ddl data type translation',
              metavar='DDLCUSTOMMAPPING',
              required=False)
@click.option('--no-apply',
              is_flag=True,
              default=False,
              help='Just output the transform without applying it into the table')
@click.argument('ddl_file')
@click.argument('transform_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def map_from(ctx: click.Context,
             ddl_file: str,
             ddl_mapping: str,
             ddl_custom_mapping: str,
             no_apply: bool,
             transform_name: str):
    the_sql = None
    with open(ddl_file) as fsql:
        the_sql = fsql.read()

    mapper = ddl_to_hdx_datatype(ddl_custom_mapping)
    create_table_info: DdlCreateTableInfo = ddl_to_create_table_info(the_sql, mapper)
    transform_dict = generate_transform_dict(create_table_info,
                                             transform_name,
                                             transform_type=create_table_info.ingest_type)
    transform_str = json.dumps(transform_dict)
    if no_apply:
        print(transform_str)
        return

    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    basic_create_with_body_from_string(user_profile,
                                       resource_path,
                                       transform_name,
                                       body_from_string=transform_str)
    print(f'Created transform {transform_name}.')

    # user_profile = ctx.parent.obj['usercontext']
    # resource_path = ctx.parent.obj['resource_path']
    # with open(body_from_file, "r", encoding="utf-8") as f:
    #     basic_create_with_body_from_string(user_profile, resource_path,
    #                                        transform_name, f.read())
    # print(f'Created transform {transform_name}.')


transform.add_command(map_from)
transform.add_command(create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
