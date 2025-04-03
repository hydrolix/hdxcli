from urllib.parse import urlparse

import click

from ...library_api.common.exceptions import HdxCliException
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import report_error_and_exit
from ...library_api.utility.file_handling import load_json_settings_file
from ..common.undecorated_click_commands import basic_create

logger = get_logger()


def any_source_impl(ctx: click.Context, source_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    project_name, table_name = user_profile.projectname, user_profile.tablename
    if not project_name or not table_name:
        raise HdxCliException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{user_profile.profilename}'"
        )

    _, table_url = access_resource_detailed(
        user_profile, [("projects", project_name), ("tables", table_name)]
    )
    table_path = urlparse(table_url).path
    sources_path = f"{table_path}sources/{source_name}/"
    ctx.obj = {"resource_path": sources_path, "usercontext": user_profile}


@click.command(
    help="Create source. 'source_filename' contains the settings. "
    "name in settings will be replaced by 'source_name'"
)
@click.argument(
    "source_filename",
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
)
@click.argument("source_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, source_filename: dict, source_name: str):
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    basic_create(user_profile, resource_path, source_name, body=source_filename)
    logger.info(f"Created source {source_name}")
