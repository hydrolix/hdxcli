"""Commands relative to project resource."""
import click

from ..common.undecorated_click_commands import basic_create
from ...library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger
from ..common.rest_operations import (
    delete as command_delete,
    list_ as command_list,
    show as command_show,
    activity as command_activity,
    stats as command_stats
)
from ..common.misc_operations import settings as command_settings
from ..common.migration import migrate_a_project

logger = get_logger()


@click.group(help="Project-related operations")
@click.option('--project', 'project_name', metavar='PROJECTNAME', default=None,
              help="Use or override project set in the profile.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def project(ctx: click.Context, project_name: str):
    user_profile = ctx.parent.obj['usercontext']
    ProfileUserContext.update_context(user_profile, projectname=project_name)
    org_id = user_profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/projects/',
               'usercontext': user_profile}


@click.command(help='Create project.')
@click.argument('project_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, project_name: str):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']
    basic_create(
        user_profile,
        resource_path,
        project_name,
        None,
        None
    )
    logger.info(f'Created project {project_name}')


@click.command(help='Migrate a project.')
@click.argument('project_name', metavar='PROJECT_NAME', required=True, default=None)
@click.option('-tp', '--target-profile', required=False, default=None)
@click.option('-h', '--target-cluster-hostname', required=False, default=None)
@click.option('-u', '--target-cluster-username', required=False, default=None)
@click.option('-p', '--target-cluster-password', required=False, default=None)
@click.option('-s', '--target-cluster-uri-scheme', required=False, default='https')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context,
            project_name: str,
            target_profile: str,
            target_cluster_hostname: str,
            target_cluster_username: str,
            target_cluster_password: str,
            target_cluster_uri_scheme: str):
    if target_profile is None and not (target_cluster_hostname and target_cluster_username
                                       and target_cluster_password and target_cluster_uri_scheme):
        raise click.BadParameter('Either provide a --target-profile or all four target cluster options.')

    user_profile = ctx.parent.obj['usercontext']
    migrate_a_project(
        user_profile,
        project_name,
        target_profile,
        target_cluster_hostname,
        target_cluster_username,
        target_cluster_password,
        target_cluster_uri_scheme
    )
    logger.info(f'Migrated project {project_name}')


project.add_command(command_list)
project.add_command(create)
project.add_command(command_delete)
project.add_command(command_show)
project.add_command(command_settings)
project.add_command(command_activity)
project.add_command(command_stats)
project.add_command(migrate)
