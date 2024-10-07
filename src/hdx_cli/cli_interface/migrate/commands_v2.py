import copy
from datetime import datetime

import click

from .data import migrate_data
from .helpers import MigrationData, get_catalog
from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from .resources import get_resources, create_resources
from .validator import validations
from hdx_cli.cli_interface.common.migration import get_target_profile
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.library_api.common.logging import get_logger
from ..profile.commands import validate_hostname

logger = get_logger()


class CustomDateTime(click.Option):
    def get_help_record(self, ctx):
        return ', '.join(self.opts), self.help


@report_error_and_exit(exctype=Exception)
def validate_tablename_format(ctx, param, value):
    if value is None or len(value.split('.')) != 2:
        raise click.BadParameter(f"'{value}' is not in the 'project_name.table_name' format.")
    return value


@click.command(help='Migrate a table and its data to a target cluster. This command allows you '
                    'to migrate Hydrolix tables, including their data, between clusters or '
                    'even within the same cluster.'
                    'The migration process creates the project, table, and transforms at '
                    'the target location. It then copies the partitions from the source bucket '
                    'to the target bucket.')
@click.argument('source_table', metavar='SOURCE_TABLE', required=True, type=str,
                callback=validate_tablename_format)
@click.argument('target_table', metavar='TARGET_TABLE', required=True, type=str,
                callback=validate_tablename_format)
@click.argument('rc_host', metavar='RCLONE_HOST', required=True, type=str,
                callback=validate_hostname)
@click.option('--target-profile', '-tp', 'target_profile_name', required=False, default=None)
@click.option('--target-hostname', '-h', required=False, default=None)
@click.option('--target-username', '-u', required=False, default=None)
@click.option('--target-password', '-p', required=False, default=None)
@click.option('--target-uri-scheme', '-s', required=False, default=None,
              type=click.Choice(['http', 'https'], case_sensitive=False))
@click.option('--allow-merge', type=bool, is_flag=True, is_eager=True, default=False,
              help='Allow migration with merge process activated in the source table. '
                   'Default is False.')
@click.option('--only', cls=CustomDateTime, type=click.Choice(['resources', 'data']),
              help='The migration type: "resources" or "data".', required=False)
@click.option('--from-date', cls=CustomDateTime, required=False,
              type=click.DateTime(formats=['%Y-%m-%d %H:%M:%S']), default=None,
              help='Minimum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.')
@click.option('--to-date', cls=CustomDateTime, required=False,
              type=click.DateTime(formats=['%Y-%m-%d %H:%M:%S']), default=None,
              help='Maximum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.')
@click.option('--reuse-partitions', type=bool, is_flag=True, default=False,
              help='Perform a dry migration without moving partitions. '
                   'Both clusters must share the bucket(s) where the partitions are stored.')
@click.option('--rc-user', type=str, required=False, default=None,
              help='The username for authenticating with the Rclone server.')
@click.option('--rc-pass', type=str, required=False, default=None,
              help='The password for authenticating with the Rclone server.')
@click.option('--concurrency', default=20, type=click.IntRange(1, 50),
              help='Number of concurrent requests during file migration. Default is 20.')
@click.option('--temp-catalog', type=bool, is_flag=True, default=False,
              help='Use a previously downloaded catalog stored in a temporary file, '
                   'instead of downloading it again.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def migrate(ctx: click.Context,
            source_table: str,
            target_table: str,
            rc_host: str,
            target_profile_name: str,
            target_hostname: str,
            target_username: str,
            target_password: str,
            target_uri_scheme: str,
            allow_merge: bool,
            only: str,
            from_date: datetime,
            to_date: datetime,
            reuse_partitions: bool,
            rc_user: str,
            rc_pass: str,
            concurrency: int,
            temp_catalog: bool
            ):
    source_profile = ctx.parent.obj['usercontext']
    if target_profile_name is None and not (
            target_hostname or target_username or target_password or target_uri_scheme
    ):
        if reuse_partitions:
            raise click.BadParameter(
                '--reuse-partitions must be used for migrations between different clusters.'
            )
        target_profile = copy.deepcopy(source_profile)

    elif target_profile_name or (
            target_hostname and target_username and target_password and target_uri_scheme
    ):
        target_profile = get_target_profile(
            target_profile_name,
            target_hostname,
            target_username,
            target_password,
            target_uri_scheme,
            source_profile.timeout
        )

    else:
        raise click.BadParameter(
            'The data provided is incorrect. Please check your input and try again.'
        )

    logger.info(f'{" Preparing Migration ":=^50}')
    source_resources = source_table.split('.')
    source_profile.projectname = source_resources[0]
    source_profile.tablename = source_resources[1]

    target_resources = target_table.split('.')
    target_profile.projectname = target_resources[0]
    target_profile.tablename = target_resources[1]

    source_data = MigrationData()
    target_data = MigrationData()
    rc_config = RcloneAPIConfig(rc_host, rc_user, rc_pass)

    # Source
    get_resources(source_profile, source_data)
    # Target
    only_storages = only != 'data'
    get_resources(target_profile, target_data, only_storages=only_storages)

    catalog = None
    if only != 'resources':
        catalog = get_catalog(source_profile, source_data, temp_catalog)

    validations(
        source_profile,
        source_data,
        target_data,
        catalog,
        from_date,
        to_date,
        only,
        allow_merge,
        reuse_partitions
    )
    logger.info('')

    # Migrations
    # 'only' parameter has 3 possible values: 'resources', 'data', None
    # with these two if statements, it handles all the possible combinations
    if only != 'data':
        create_resources(
            target_profile,
            target_data,
            source_profile,
            source_data,
            reuse_partitions
        )
    if only != 'resources':
        migrate_data(
            target_profile,
            target_data,
            source_data.storages,
            catalog,
            rc_config,
            concurrency,
            reuse_partitions
        )

    logger.info(f'{" Migration Process Completed ":=^50}')
