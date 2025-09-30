import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create_file,
    basic_delete,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, skip_group_logic_on_help
from hdx_cli.library_api.utility.file_handling import read_bytes_from_file

from .operations import download_dictionary_file

logger = get_logger()


@click.group(cls=HdxGroup, name="files")
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
def files_group(ctx: click.Context):
    """Manage dictionary data files."""
    user_profile = ctx.parent.obj["usercontext"]
    base_path = ctx.obj["resource_path"]
    resource_path = f"{base_path}files"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@click.command(cls=HdxCommand, name="upload")
@click.argument(
    "file_path_to_upload",
    metavar="FILE_PATH_TO_UPLOAD",
    type=click.Path(exists=True, readable=True),
)
@click.argument("dict_file_name", metavar="DICT_FILE_NAME")
@click.option(
    "--body-from-file-type",
    "-t",
    type=click.Choice(("json", "verbatim"), case_sensitive=False),
    help="How to interpret the body from the file. Defaults to 'json'.",
    default="json",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def upload(
    ctx: click.Context,
    file_path_to_upload: str,
    dict_file_name: str,
    body_from_file_type: str,
):
    """Upload a dictionary data file.

    \b
    Examples:
      # Upload a local CSV file to be used as a data source for a dictionary
      hdxcli dictionary --project my_project files upload ./local_countries.csv countries -t verbatim
    """
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    file_content = read_bytes_from_file(file_path_to_upload)
    basic_create_file(
        profile,
        resource_path,
        dict_file_name,
        file_content=file_content,
        file_type=body_from_file_type,
    )
    logger.info(f"Uploaded dictionary file {dict_file_name}")


@click.command(cls=HdxCommand, name="download")
@click.argument("dictionary_filename", metavar="DICTIONARY_FILENAME")
@click.option(
    "--output",
    "-o",
    "output_path",
    help="Path to save the file, including the new filename. "
    "If not provided, saves to the current directory with the original name.",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def download(ctx: click.Context, dictionary_filename: str, output_path: str):
    """
    Download a dictionary data file to your local machine.
    This command retrieves a dictionary file and saves it
    to a specified path, or the current directory by default.

    \b
    Examples:
      # Download 'countries' to the current directory
      hdxcli dictionary --project my_proj files download countries

    \b
      # Download 'countries' but save it as 'country_list.csv' in the current dir
      hdxcli dictionary --project my_proj files download countries.csv -o country_list.csv

    \b
      # Download 'countries' to a specific 'data' folder with 'countries.csv' as the new filename
      hdxcli dictionary --project my_proj files download countries.csv --output ./data/countries.csv
    """
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    download_dictionary_file(profile, resource_path, dictionary_filename, output_path)


@click.command(cls=HdxCommand, name="delete")
@click.argument("file_name", metavar="FILE_NAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, file_name: str):
    """Delete a dictionary data file.

    \b
    Examples:
      # Delete the file named 'my_dictionary_file'
      hdxcli dictionary --project my_project files delete my_dictionary_file
    """
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    hostname = profile.hostname
    scheme = profile.scheme
    resource_url = f"{scheme}://{hostname}{resource_path}/{file_name}"
    basic_delete(profile, resource_path, file_name, url=resource_url)
    logger.info(f"Deleted dictionary file {file_name}")


files_group.add_command(upload)
files_group.add_command(download)
files_group.add_command(delete)
files_group.add_command(command_list)
