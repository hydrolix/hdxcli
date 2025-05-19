"""Commands relative to project resource."""

import click

from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    ensure_logged_in,
    no_rollback_option,
    report_error_and_exit,
    target_cluster_options,
)
from ...models import ProfileUserContext
from ..common.migration.resource_migrations import migrate_resource_config
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import activity as command_activity
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.rest_operations import stats as command_stats
from ..common.undecorated_click_commands import basic_create

logger = get_logger()


@click.group(help="Project-related operations")
@click.option(
    "--project",
    "project_name",
    metavar="PROJECTNAME",
    default=None,
    help="Use or override project set in the profile.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def project(ctx: click.Context, project_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, projectname=project_name)
    org_id = user_profile.org_id
    ctx.obj = {"resource_path": f"/config/v1/orgs/{org_id}/projects/", "usercontext": user_profile}


@click.command(help="Create project.")
@click.argument("project_name", metavar="PROJECTNAME", required=True)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, project_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, project_name)
    logger.info(f"Created project {project_name}")


@click.command(
    help=(
        "Migrate a project and its associated resources.\n\n"
        "This command migrates a project from the source profile to the specified target profile "
        "or cluster. By default, all resources associated with the project are also migrated, "
        "including tables (and their associated transforms).\n\n"
        "Options allow you to customize the migration:\n"
        "- Use --dictionaries (-D) to include dictionaries in the migration.\n"
        "- Use --functions (-F) to include functions in the migration.\n"
        "- Use --only (-O) to migrate only the project, skipping all dependencies.\n\n"
        "Provide a target profile using --target-profile or specify cluster details "
        "(hostname, username, password, and URI scheme)."
    )
)
@click.argument("new_project_name", metavar="NEW_PROJECT_NAME", required=True, default=None)
@target_cluster_options
@no_rollback_option
@click.option(
    "-O",
    "--only",
    required=False,
    default=False,
    is_flag=True,
    help="Migrate only the project, skipping dependencies.",
)
@click.option(
    "-D",
    "--dictionaries",
    required=False,
    default=False,
    is_flag=True,
    help="Migrate dictionaries associated with the project.",
)
@click.option(
    "-F",
    "--functions",
    required=False,
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
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.projectname:
        raise click.BadParameter("No source project name provided.")
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
