import click

from ...library_api.utility.decorators import report_error_and_exit
from ..common.undecorated_click_commands import basic_create_with_body_from_string


def any_source_impl(ctx: click.Context, source_name):
    profileinfo = ctx.parent.obj.get('usercontext')
    sources_path = ctx.parent.obj.get('resource_path')
    sourcename_resource_path = f'{sources_path}{source_name}/'
    ctx.obj = {'usercontext': profileinfo,
               'resource_path': sourcename_resource_path}


@click.command(help="Create source. 'source_filename' contains the settings. "
                    "name in settings will be replaced by 'source_name'")
@click.argument('source_filename')
@click.argument('source_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context,
           source_filename: str,
           source_name: str):
    user_profile = ctx.parent.obj.get('usercontext')
    resource_path = ctx.parent.obj.get('resource_path')
    with open(source_filename, "r", encoding="utf-8") as f:
        basic_create_with_body_from_string(user_profile, resource_path,
                                           source_name, f.read())
    print(f'Created source {source_name}.')
