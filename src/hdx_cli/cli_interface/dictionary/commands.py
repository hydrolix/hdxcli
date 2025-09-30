import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand, HdxGroup
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
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
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

from .files import files_group as files

logger = get_logger()


@click.group(cls=HdxGroup, name="dictionary")
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


@click.command(cls=HdxCommand, name="create")
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


@click.command(cls=HdxCommand, name="migrate")
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


# Subgroup
dictionary.add_command(files)
# Commands
dictionary.add_command(create)
dictionary.add_command(command_list)
dictionary.add_command(command_delete)
dictionary.add_command(command_show)
dictionary.add_command(command_settings)
dictionary.add_command(migrate)
