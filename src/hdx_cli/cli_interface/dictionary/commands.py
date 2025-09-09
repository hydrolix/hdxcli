import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create,
    basic_create_file,
    basic_delete,
)
from hdx_cli.library_api.common.exceptions import (
    MissingSettingsException,
    ResourceNotFoundException,
)
from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from hdx_cli.library_api.utility.file_handling import read_bytes_from_file, read_json_from_file
from hdx_cli.models import ProfileUserContext

from .operations import download_dictionary_file

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECT_NAME",
    default=None,
)
@click.option(
    "--dictionary",
    "dictionary_name",
    help="Perform operation on the passed dictionary.",
    metavar="DICTIONARY_NAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def dictionary(ctx: click.Context, project_name: str, dictionary_name: str):
    """This group of commands allows creating, listing, showing, deleting,
    and migrating dictionaries. A project context is required for all
    operations."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, dictionaryname=dictionary_name
    )

    project_name = user_profile.projectname
    if not project_name:
        raise ResourceNotFoundException(
            f"No project parameter provided and "
            f"no project set in profile '{user_profile.profilename}'"
        )

    project_body = access_resource(user_profile, [("projects", project_name)])
    project_id = project_body.get("uuid")
    org_id = user_profile.org_id
    resource_path = f"/config/v1/orgs/{org_id}/projects/{project_id}/dictionaries/"
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@click.command(cls=HdxCommand)
@click.argument(
    "dict_settings_file_path",
    metavar="DICT_SETTINGS_FILE_PATH",
    type=click.Path(exists=True, readable=True),
)
@click.argument("dict_file_name", metavar="DICT_FILE_NAME")
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    dict_settings_file_path: str,
    dict_file_name: str,
    resource_name: str,
):
    """Create a new {resource} definition.

    This command creates a {resource} by combining a settings file
    with the name of a data file that has been previously uploaded.

    \b
    **Arguments**:
    - `SETTINGS_FILE_PATH`: Path to a JSON file with dictionary settings.
    - `DICT_FILE_NAME`: The name of the data file already uploaded via `files upload`.
    - `DICTIONARY_NAME`: The name for the new dictionary.

    \b
    Examples:
      # Create a dictionary named 'country_codes' using 'countries' and a settings file
      {full_command_prefix} create ./settings.json countries country_codes
    """
    profile = ctx.obj["usercontext"]
    resource_path = ctx.obj["resource_path"]
    body = read_json_from_file(dict_settings_file_path)

    if not body.get("settings"):
        raise MissingSettingsException("Missing 'settings' field in 'DICT_SETTINGS_FILE_PATH'")

    body["settings"]["filename"] = dict_file_name
    basic_create(profile, resource_path, resource_name, body=body)
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME")
@click.argument("new_dictionary_name", metavar="NEW_DICTIONARY_NAME")
@target_cluster_options
@no_rollback_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    target_project_name: str,
    new_dictionary_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
):
    """Migrate a {resource} to a different project.

    Migrates a {resource} from a source context (in the current profile)
    to a target project, which can be in the same or a different cluster.
    Authentication for the target cluster can be provided via a separate profile
    using `--target-profile` or by specifying credentials directly.

    By default, any failure during the process will trigger a rollback of the
    changes made. Use the `--no-rollback` flag to disable this behavior.

    \b
    Examples:
      # Migrate '{example_name}' to a new project 'my_target_project'
      {full_command_prefix} --{resource} {example_name} migrate my_target_project my_new_{resource}
    """
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.dictionaryname:
        raise click.BadParameter(
            "A source dictionary must be specified with the --dictionary option.",
            param_hint="--dictionary",
        )

    has_target_profile = target_profile is not None
    has_all_cluster_options = all(
        [
            target_cluster_hostname,
            target_cluster_username,
            target_cluster_password,
            target_cluster_uri_scheme,
        ]
    )

    if not has_target_profile and not has_all_cluster_options:
        raise click.BadParameter(
            "Either provide a --target-profile or all four target cluster options."
        )

    data = {
        "source_profile": source_profile,
        "target_profile_name": target_profile,
        "target_cluster_hostname": target_cluster_hostname,
        "target_cluster_username": target_cluster_username,
        "target_cluster_password": target_cluster_password,
        "target_cluster_uri_scheme": target_cluster_uri_scheme,
        "source_project": source_profile.projectname,
        "target_project": target_project_name,
        "source_dictionary": source_profile.dictionaryname,
        "target_dictionary": new_dictionary_name,
        "no_rollback": no_rollback,
    }
    migrate_resource_config("dictionary", **data)

    logger.info("All resources migrated successfully")


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
def download_file(ctx: click.Context, dictionary_filename: str, output_path: str):
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
      hdxcli dictionary --project my_proj files download countries.csv -o ./data/countries.csv
    """
    profile = ctx.parent.obj["usercontext"]
    # The resource path is for 'files', which is what the operation function expects
    resource_path = ctx.parent.obj["resource_path"]
    download_dictionary_file(profile, resource_path, dictionary_filename, output_path)


@click.group(cls=HdxGroup)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
def files(ctx: click.Context):
    """Manage dictionary data files."""
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = f'{ctx.obj["resource_path"]}files'
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@click.command(cls=HdxCommand)
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
def files_upload(
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


@click.command(cls=HdxCommand)
@click.argument("file_name", metavar="FILE_NAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def files_delete(ctx: click.Context, file_name: str):
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


dictionary.add_command(create, name="create")
dictionary.add_command(files)
files.add_command(files_upload, name="upload")
files.add_command(command_list)
files.add_command(download_file)
files.add_command(files_delete, name="delete")

dictionary.add_command(command_list)
dictionary.add_command(command_delete)
dictionary.add_command(command_show)
dictionary.add_command(command_settings)
dictionary.add_command(migrate)
