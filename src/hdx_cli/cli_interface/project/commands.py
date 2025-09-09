import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.migration.resource_migrations import migrate_resource_config
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import activity as command_activity
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.rest_operations import stats as command_stats
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import (
    report_error_and_exit,
    ensure_logged_in,
    target_cluster_options,
    no_rollback_option,
)
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--project",
    "project_name",
    metavar="PROJECT_NAME",
    default=None,
    help="Use or override project set in the profile.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def project(ctx: click.Context, project_name: str):
    """Provides commands to create, list, show, delete, and migrate
    projects. It also includes tools for managing project settings
    and viewing activity logs."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, projectname=project_name)
    org_id = user_profile.org_id
    ctx.obj = {"resource_path": f"/config/v1/orgs/{org_id}/projects/", "usercontext": user_profile}


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, resource_name: str):
    """Creates a new, empty {resource} in your Hydrolix cluster.

    \b
    Examples:
      # Create a new {resource} named 'my_project'
      {full_command_prefix} create {example_name}
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, resource_name)
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


@click.command(cls=HdxCommand)
@click.argument("new_project_name")
@target_cluster_options
@no_rollback_option
@click.option(
    "-O",
    "--only",
    default=False,
    is_flag=True,
    help="Migrate only the project, skipping dependencies.",
)
@click.option(
    "-D",
    "--dictionaries",
    default=False,
    is_flag=True,
    help="Migrate dictionaries associated with the project.",
)
@click.option(
    "-F",
    "--functions",
    default=False,
    is_flag=True,
    help="Migrate functions associated with the project.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    new_project_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
    only: bool,
    dictionaries: bool,
    functions: bool,
):
    """Migrate a {resource} and its associated resources.

    This command migrates a {resource} from the source profile to a specified
    target profile or cluster. By default, all associated resources are also
    migrated (e.g., tables and their transforms).
    Authentication for the target cluster can be provided via a separate profile
    using `--target-profile` or by specifying credentials directly.

    \b
    Options allow for customizing the migration:
    - `--dictionaries`: Include associated dictionaries.
    - `--functions`: Include associated functions.
    - `--only`: Migrate only the {resource}, skipping all dependencies.

    By default, any failure during the process will trigger a rollback of the
    changes made. Use the `--no-rollback` flag to disable this behavior.

    \b
    Examples:
      # Migrate '{example_name}' to 'new_proj' on a target profile
      {full_command_prefix} --{resource} {example_name} migrate new_proj --target-profile prod_cluster

    \b
      # Migrate only the {resource} '{example_name}' and its dictionaries, on a target profile
      {full_command_prefix} --{resource} {example_name} migrate new_proj --only --dictionaries --target-profile prod_cluster
    """
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.projectname:
        raise click.BadParameter("A source project must be specified with the --project option.",
                                 param_hint="--project")

    has_target_profile = target_profile is not None
    has_all_cluster_options = all([target_cluster_hostname, target_cluster_username,
                                   target_cluster_password, target_cluster_uri_scheme])
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
        "target_project": new_project_name,
        "no_rollback": no_rollback,
        "only": only,
        "dicts": dictionaries,
        "functs": functions,
    }
    migrate_resource_config("project", **data)

    logger.info("All resources migrated successfully")


project.add_command(command_list)
project.add_command(create)
project.add_command(command_delete)
project.add_command(command_show)
project.add_command(command_settings)
project.add_command(command_activity)
project.add_command(command_stats)
project.add_command(migrate)
