import copy
from datetime import datetime

import click

from hdx_cli.cli_interface.common.migration import get_target_profile
from hdx_cli.cli_interface.migrate.data import migrate_data
from hdx_cli.cli_interface.migrate.helpers import MigrationData, get_catalog
from hdx_cli.cli_interface.migrate.resources import get_resources, creating_resources
from hdx_cli.cli_interface.migrate.validator import validates
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()


class CustomDateTime(click.Option):
    def get_help_record(self, ctx):
        return ', '.join(self.opts), self.help


@report_error_and_exit(exctype=Exception)
def validate_tablename_format(ctx, param, value):
    if value is None or len(value.split('.')) != 2:
        raise click.BadParameter(f"'{value}' is not in the 'project_name.table_name' format.")
    return value


@click.command(name='migrate')
@click.option('--target-profile', '-tp', 'target_profile_name', required=False, default=None)
@click.option('--target-hostname', '-h', required=False, default=None)
@click.option('--target-username', '-u', required=False, default=None)
@click.option('--target-password', '-p', required=False, default=None)
@click.option('--target-uri-scheme', '-s', required=False, default=None,
              type=click.Choice(['http', 'https'], case_sensitive=False))
@click.option('--source-table', callback=validate_tablename_format, required=True, default=None,
              help='The source table in the format "project.table" to migrate from.')
@click.option('--target-table', callback=validate_tablename_format, required=True, default=None,
              help='The target table in the format "project.table" to migrate to.')
@click.option('--allow-merge', type=bool, is_flag=True, is_eager=True, default=False,
              help='Allow migration with merge process activated in the source table. '
                   'Default is False.')
@click.option('--only', cls=CustomDateTime, type=click.Choice(['resources', 'data']),
              help='The migration type: "resources" or "data".', required=False)
@click.option('--min-timestamp', cls=CustomDateTime, required=False,
              type=click.DateTime(formats=['%Y-%m-%d %H:%M:%S']), default=None,
              help='Minimum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.')
@click.option('--max-timestamp', cls=CustomDateTime, required=False,
              type=click.DateTime(formats=['%Y-%m-%d %H:%M:%S']), default=None,
              help='Maximum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.')
@click.option('--recovery', type=bool, is_flag=True, default=False,
              help='Continue a previous migration that did not complete successfully.')
@click.option('--reuse-partitions', type=bool, is_flag=True, default=False,
              help="Perform a dry migration without moving partitions. "
                   "Both clusters must share the bucket(s) where the partitions are stored.")
@click.option('--workers', type=click.IntRange(1, 50), default=10,
              help='Number of worker threads to use for migrating partitions. Default is 10.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context, target_profile_name: str, target_hostname: str,
            target_username: str, target_password: str, target_uri_scheme: str,
            source_table: str, target_table: str, allow_merge: bool, only: str,
            min_timestamp: datetime, max_timestamp: datetime, recovery: bool,
            reuse_partitions: bool, workers: int):
    source_profile = ctx.parent.obj['usercontext']

    if (target_profile_name is None and
            not (target_hostname or target_username or target_password or target_uri_scheme)):
        if reuse_partitions:
            raise click.BadParameter(
                '--reuse-partitions must be used for migrations between different clusters.')
        target_profile = copy.deepcopy(source_profile)

    elif (target_profile_name or
          (target_hostname and target_username and target_password and target_uri_scheme)):
        target_profile = get_target_profile(target_profile_name, target_hostname,
                                            target_username, target_password, target_uri_scheme,
                                            source_profile.timeout)

    else:
        raise click.BadParameter(
            'The data provided is incorrect. Please check your input and try again.')

    logger.info(f'{" Preparing Migration ":=^50}')
    # Source table name
    source_resources = source_table.split('.')
    source_profile.projectname = source_resources[0]
    source_profile.tablename = source_resources[1]

    # Target table name
    target_resources = target_table.split('.')
    target_profile.projectname = target_resources[0]
    target_profile.tablename = target_resources[1]

    source_data = MigrationData()
    target_data = MigrationData()

    # Obtaining resources to migrate
    # Source
    get_resources(source_profile, source_data)
    # Target
    only_storages = not (only == 'data' or recovery)
    get_resources(target_profile, target_data, only_storages=only_storages)

    if only != 'resources':
        # Getting catalog
        catalog = get_catalog(source_profile, source_data)
        # Running validations
        validates(source_profile, source_data, target_data, catalog,
                  min_timestamp, max_timestamp, allow_merge, reuse_partitions)

    logger.info('')

    # Migrations
    if only == 'resources' and not recovery:
        creating_resources(target_profile, target_data, source_data, reuse_partitions)
    elif only == 'data' or recovery:
        migrate_data(target_profile, target_data, source_data.storages,
                     catalog, workers, recovery, reuse_partitions)
    else:
        creating_resources(target_profile, target_data, source_data, reuse_partitions)
        migrate_data(target_profile, target_data, source_data.storages,
                     catalog, workers, recovery, reuse_partitions)

    logger.info(f'{" Migration Process Completed ":=^50}')
    logger.info('')
