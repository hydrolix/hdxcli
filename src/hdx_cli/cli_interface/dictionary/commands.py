"""Commands relative to dictionary handling operations"""

import click

from ...library_api.common.exceptions import (
    MissingSettingsException,
    ResourceNotFoundException,
)
from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from ...library_api.utility.file_handling import load_bytes_file, load_json_settings_file
from ...models import ProfileUserContext
from ..common.migration.resource_migrations import migrate_resource_config
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create, basic_create_file, basic_delete

logger = get_logger()


@click.group(help="Dictionary-related operations")
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--dictionary",
    "dictionary_name",
    help="Perform operation on the passed dictionary.",
    metavar="DICTIONARYNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def dictionary(ctx: click.Context, project_name: str, dictionary_name: str):
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


@click.group(help="Files operations")
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
def files(ctx: click.Context):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = f'{ctx.obj["resource_path"]}files'
    ctx.obj = {"resource_path": resource_path, "usercontext": user_profile}


@click.command(
    help="Create dictionary. 'dictionary_settings_file' contains the settings of "
    "the dictionary. The filename and name in settings will be replaced by "
    "'dictionary_filename' and 'dictionary_name' respectively."
)
@click.argument(
    "dictionary_settings_file",
    metavar="DICTIONARYSETTINGSFILE",
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
)
@click.argument("dictionary_filename")
@click.argument("dictionary_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create_dict(
    ctx: click.Context,
    dictionary_settings_file: dict,
    dictionary_filename: str,
    dictionary_name: str,
):
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]

    if not dictionary_settings_file.get("settings"):
        raise MissingSettingsException("Missing 'settings' field in 'DICTIONARYSETTINGSFILE'")

    dictionary_settings_file["settings"]["filename"] = dictionary_filename
    basic_create(profile, resource_path, dictionary_name, body=dictionary_settings_file)
    logger.info(f"Created {dictionary_name}")


@click.command(help="Upload a dictionary file.")
@click.option(
    "--body-from-file-type",
    "-t",
    type=click.Choice(("json", "verbatim")),
    help="How to interpret the body from option. ",
    metavar="BODYFROMFILETYPE",
    default="json",
)
@click.argument(
    "dictionary_file_to_upload",
    metavar="DICTIONARYFILE",
    type=click.Path(exists=True, readable=True),
    callback=load_bytes_file,
)
@click.argument("dictionary_filename", metavar="DICTIONARYFILENAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def upload_file_dict(
    ctx: click.Context,
    dictionary_file_to_upload: bytes,
    dictionary_filename: str,
    body_from_file_type: str,
):
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create_file(
        profile,
        resource_path,
        dictionary_filename,
        file_content=dictionary_file_to_upload,
        file_type=body_from_file_type,
    )
    logger.info(f"Uploaded dictionary file {dictionary_filename}")


@click.command(help="Delete dictionary file.")
@click.argument("dictionary_filename")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def dict_file_delete(ctx: click.Context, dictionary_filename):
    profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    hostname = profile.hostname
    scheme = profile.scheme
    resource_url = f"{scheme}://{hostname}{resource_path}/{dictionary_filename}"
    basic_delete(profile, resource_path, dictionary_filename, url=resource_url)
    logger.info(f"Deleted {dictionary_filename}")


@click.command(
    help=(
        "Migrate a dictionary to a target project and profile.\n\n"
        "This command migrates a dictionary from the current source profile to the specified "
        "target project and target profile. The target profile can be provided directly using "
        "the --target-profile option, or by specifying the target cluster details such as "
        "hostname, username, password, and URI scheme."
    ),
    name="migrate",
)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME", required=True, default=None)
@click.argument("new_dictionary_name", metavar="NEW_DICTIONARY_NAME", required=True, default=None)
@target_cluster_options
@no_rollback_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate_dictionary(
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
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.dictionaryname:
        raise click.BadParameter("No source dictionary provided.")
    if target_profile is None and not (
        target_cluster_hostname
        and target_cluster_username
        and target_cluster_password
        and target_cluster_uri_scheme
    ):
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


dictionary.add_command(create_dict, name="create")
dictionary.add_command(files)
files.add_command(upload_file_dict, name="upload")
files.add_command(command_list)
files.add_command(dict_file_delete, name="delete")

dictionary.add_command(command_list)
dictionary.add_command(command_delete)
dictionary.add_command(command_show)
dictionary.add_command(command_settings)
dictionary.add_command(migrate_dictionary)
