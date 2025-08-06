import click

from hdx_cli.cli_interface.common.cached_operations import find_transforms
from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.exceptions import ResourceNotFoundException, LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from hdx_cli.library_api.utility.file_handling import read_bytes_from_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()


def _get_content_type(obj_type):
    content_types = {"csv": "text/csv", "json": "application/json"}
    return content_types.get(obj_type, "text/csv")


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECT_NAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLE_NAME",
    default=None,
)
@click.option(
    "--transform",
    "transform_name",
    help="Explicitly pass the transform name. If none is given, "
    "the default transform for the used table is used.",
    metavar="TRANSFORM_NAME",
    default=None,
)
@click.pass_context
@ensure_logged_in
def stream(ctx: click.Context, project_name: str, table_name: str, transform_name: str):
    """Commands for streaming data ingestion into a table.
    A project and table context is required."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, transformname=transform_name
    )
    stream_path = "/ingest/event"
    ctx.obj = {"resource_path": stream_path, "usercontext": user_profile}


@click.command(cls=HdxCommand)
@click.argument(
    "data_file_path",
    metavar="DATA_FILE_PATH",
    type=click.Path(exists=True, readable=True)
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def ingest(ctx: click.Context, data_file_path: str):
    """Ingest data from a file into a table.

    This command sends the contents of a local file to the ingest endpoint.
    It automatically applies the default transform for the target table,
    unless a specific transform is provided via the `--transform` option.

    \b
    Examples:
      # Ingest data from a JSON file using the default transform
      {full_command_prefix} ingest /data/today/access_log.json

    \b
      # Ingest CSV data using a specific transform
      {full_command_prefix} --transform custom_csv_transform ingest /data/today/metrics.csv
    """
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    if not user_profile.projectname or not user_profile.tablename:
        raise LogicException("A project and table must be specified "
                             "via options or set in the current profile.")

    stream_data_bytes = read_bytes_from_file(data_file_path)

    # Find the transform to use, either specified or default
    explicit_transform_name = user_profile.transformname
    transforms_list = find_transforms(user_profile)

    transform_to_use = None
    if explicit_transform_name:
        transform_to_use = next((t for t in transforms_list if t["name"] == explicit_transform_name), None)
        if not transform_to_use:
            raise ResourceNotFoundException(f"Transform '{explicit_transform_name}' not found.")
    else:
        transform_to_use = next((t for t in transforms_list if t.get("settings", {}).get("is_default")), None)
        if not transform_to_use:
            raise ResourceNotFoundException(
                "No default transform found for the table. Please specify one with --transform.")

    transform_name = transform_to_use["name"]
    transform_type = transform_to_use["type"]

    extra_headers = {
        "content-type": _get_content_type(transform_type),
        "x-hdx-table": f"{user_profile.projectname}.{user_profile.tablename}",
        "x-hdx-transform": transform_name,
    }
    basic_create(
        user_profile,
        resource_path,
        body=stream_data_bytes,
        body_type="bytes",
        extra_headers=extra_headers,
    )
    logger.info(f"Successfully ingested data from '{data_file_path}'")


stream.add_command(ingest)
