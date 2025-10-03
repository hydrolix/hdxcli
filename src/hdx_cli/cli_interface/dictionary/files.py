import click
from InquirerPy import prompt
from InquirerPy.validator import EmptyInputValidator

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create_file,
    basic_delete,
    generic_basic_list,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, skip_group_logic_on_help
from hdx_cli.library_api.utility.file_handling import read_bytes_from_file

from ...models import ProfileUserContext
from .operations import download_dictionary_file

logger = get_logger()


def _delete_file_logic(profile: ProfileUserContext, resource_path: str, file_name: str):
    """Core logic to delete a dictionary file."""
    hostname = profile.hostname
    scheme = profile.scheme
    resource_url = f"{scheme}://{hostname}{resource_path}/{file_name}"
    basic_delete(profile, resource_path, file_name, url=resource_url)
    logger.info(f"Deleted dictionary file {file_name}")


def _upload_file_logic(
    profile: ProfileUserContext, resource_path: str, local_path: str, remote_name: str
):
    """Core logic to upload a dictionary file."""
    file_content = read_bytes_from_file(local_path)
    basic_create_file(
        profile,
        resource_path,
        remote_name,
        file_content=file_content,
    )
    logger.info(f"Uploaded dictionary file {remote_name}")


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
    _upload_file_logic(profile, resource_path, file_path_to_upload, dict_file_name)


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
    _delete_file_logic(profile, resource_path, file_name)


@click.command(cls=HdxCommand, name="update")
@click.argument(
    "local_file_path",
    metavar="LOCAL_FILE_PATH",
    type=click.Path(exists=True, readable=True, resolve_path=True),
)
@click.argument("remote_file_name", metavar="REMOTE_FILE_NAME", required=False)
@click.option(
    "-y",
    "--yes",
    is_flag=True,
    help="Skip the confirmation prompt and proceed with the update.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def update(
    ctx: click.Context,
    local_file_path: str,
    remote_file_name: str,
    yes: bool,
):
    """Updates an existing dictionary data file by replacing it.

    This command provides a convenient way to replace a remote dictionary
    file with a local version in a single step. It first deletes the
    existing remote file and then uploads the new one.

    \b
    The command can be run in two modes:
     - INTERACTIVE: If `REMOTE_FILE_NAME` is not provided, you will be
       prompted to select from a list of existing files.
     - DIRECT: If `REMOTE_FILE_NAME` is provided, the command will target
       that file directly. For safety, a confirmation prompt is still
       shown unless the `--yes` flag is used.

    \b
    Examples:
      # Interactively update a dictionary file in the 'my_project' project
      hdxcli dictionary --project my_project files update ./new_countries.csv

    \b
      # Directly update 'countries' file after asking for confirmation
      hdxcli dictionary --project my_project files update ./new_countries.csv countries

    \b
      # Directly update 'cities' file without any prompts
      hdxcli dictionary --project my_project files update ./new_cities.json cities --yes
    """
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    file_to_replace = remote_file_name

    remote_files = generic_basic_list(profile, resource_path)
    if not remote_files:
        raise click.ClickException(
            f"No dictionary files found in project '{profile.projectname}' to update."
        )

    if not file_to_replace:
        if yes:
            raise click.UsageError(
                "Cannot use --yes in interactive mode. Please provide REMOTE_FILE_NAME."
            )

        questions = [
            {
                "type": "list",
                "message": "Select the dictionary file to replace:",
                "choices": remote_files,
                "name": "file_to_replace",
                "validate": EmptyInputValidator(),
            }
        ]
        result = prompt(questions=questions)
        if not result:
            raise click.Abort()
        file_to_replace = result["file_to_replace"]
    else:
        if file_to_replace not in remote_files:
            raise click.ClickException(
                f"File '{file_to_replace}' not found in project '{profile.projectname}'."
            )

    if not yes:
        confirm_questions = [
            {
                "type": "confirm",
                "message": f"This will permanently replace '{file_to_replace}' in project '{profile.projectname}'."
                f"\n  Are you sure you want to continue?",
                "name": "confirm",
                "default": False,
            }
        ]
        confirm_result = prompt(questions=confirm_questions)
        if not confirm_result or not confirm_result.get("confirm"):
            logger.info("Update cancelled by user.")
            return

    logger.info(f"Replacing '{file_to_replace}'...")
    _delete_file_logic(profile, resource_path, file_to_replace)
    _upload_file_logic(profile, resource_path, local_file_path, file_to_replace)
    logger.info(f"Successfully updated dictionary file '{file_to_replace}'")


files_group.add_command(upload)
files_group.add_command(download)
files_group.add_command(delete)
files_group.add_command(update)
files_group.add_command(command_list)
