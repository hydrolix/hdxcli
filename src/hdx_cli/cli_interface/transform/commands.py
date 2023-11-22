"""Transform resource command. It flows down url for current table"""
from typing import Optional
import json
import click

from ..common.undecorated_click_commands import basic_transform
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.common.exceptions import CommandLineException
from ...library_api.common.context import ProfileUserContext

from ..common.rest_operations import (delete as command_delete,
                                      list_ as command_list,
                                      show as command_show)

from ...library_api.ddl.common_algo import (ddl_to_create_table_info,
                                            ddl_datatype_to_hdx_datatype,
                                            generate_transform_dict)

from ...library_api.ddl.common_intermediate_representation import DdlCreateTableInfo

from ..common.misc_operations import settings as command_settings
from ..common.undecorated_click_commands import basic_create_with_body_from_string


@click.group(help="Transform-related operations")
@click.option('--project', 'project_name', help="Use or override project set in the profile.",
              metavar='PROJECTNAME', default=None)
@click.option('--table', 'table_name', help="Use or override table set in the profile.",
              metavar='TABLENAME', default=None)
@click.option('--transform', 'transform_name',
              help="Explicitly pass the transform name. If none is given, "
                   "the default transform for the used table is used.",
              metavar='TRANSFORMNAME', default=None)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def transform(ctx: click.Context,
              project_name,
              table_name,
              transform_name):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile,
                                      projectname=project_name,
                                      tablename=table_name,
                                      transformname=transform_name)
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
    with open(body_from_file, "r", encoding="utf-8") as file:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           transform_name, file.read())
    print(f'Created transform {transform_name}')


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
@click.option('--source-mapping-ddl-name', '-s',
              help='The language used in the source mapping. For sql it can be autodected '
              'from the the ddl_file extension. Use when disambiguation is needed.',
              metavar='SOURCEMAPPING',
              required=False,
              default=None)
@click.option('--user-choices',
              help='Json file with user choices. These are applied to your mapping as a postprocess step',
              metavar='USERCHOICESFILE',
              required=False)
@click.argument('ddl_file')
@click.argument('transform_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint: disable=too-many-arguments
def map_from(ctx: click.Context,
             ddl_file: str,
             ddl_mapping: Optional[str],
             ddl_custom_mapping: Optional[str],
             no_apply: bool,
             source_mapping_ddl_name: Optional[str],
             user_choices: Optional[str],
             transform_name: str):
    if source_mapping_ddl_name is None:
        if ddl_file.lower().endswith('.sql'):
            source_mapping_ddl_name = 'sql'
    if not source_mapping_ddl_name:
        raise CommandLineException(f'Unkown source mapping dsl for {ddl_file}. '
                                   'Please specify one with -s option.')

    ddl_file_contents = None
    with open(ddl_file, encoding='utf-8') as ddl_stream:
        ddl_file_contents = ddl_stream.read()

    mapper = ddl_datatype_to_hdx_datatype(ddl_custom_mapping, source_mapping_ddl_name)
    create_table_info: DdlCreateTableInfo = ddl_to_create_table_info(ddl_file_contents,
                                                                     source_mapping_ddl_name,
                                                                     mapper,
                                                                     user_choices_file=user_choices)
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
    print(f'Created transform {transform_name}')


transform.add_command(map_from)
transform.add_command(create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings)
