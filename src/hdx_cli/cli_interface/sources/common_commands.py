from urllib.parse import urlparse

import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.exceptions import HdxCliException
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.utility.file_handling import read_json_from_file

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


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.argument(
    "settings_filename",
    required=True,
    default=None,
    type=click.Path(exists=True, readable=True),
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, settings_filename: str, resource_name: str):
    """Creates a new {resource} source from a JSON configuration file.

    \b
    Examples:
      # Create a {resource} source
      {full_command_prefix} create {example_name} path/to/{resource}-settings.json
    """
    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    settings_body = read_json_from_file(settings_filename)
    basic_create(user_profile, resource_path, resource_name, body=settings_body)
    logger.info(f"Created {ctx.parent.command.name} source {resource_name}")
