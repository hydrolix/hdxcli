from datetime import datetime

import click

from hdx_cli.auth.context_builder import get_profile
from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.migrate.data import migrate_data
from hdx_cli.cli_interface.migrate.helpers import MigrationData, get_catalog
from hdx_cli.cli_interface.migrate.rc.rc_manager import RcloneAPIConfig
from hdx_cli.cli_interface.migrate.resources import get_resources, create_resources
from hdx_cli.cli_interface.migrate.validator import validations
from hdx_cli.config.profile_settings import is_valid_hostname
from hdx_cli.library_api.common.exceptions import InvalidHostnameException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in

logger = get_logger()


class CustomDateTime(click.Option):
    def get_help_record(self, ctx):
        return ", ".join(self.opts), self.help


@report_error_and_exit(exctype=Exception)
def validate_tablename_format(ctx, param, value):
    if value is None or len(value.split(".")) != 2:
        raise click.BadParameter(f"'{value}' is not in the 'project_name.table_name' format.")
    return value


@report_error_and_exit(exctype=Exception)
def validate_hostname(ctx, params, hostname: str) -> str:
    if hostname and not is_valid_hostname(hostname):
        raise InvalidHostnameException("Invalid host name format.")
    return hostname


@click.command(cls=HdxCommand)
@click.argument(
    "source_table",
    metavar="SOURCE_TABLE",
    callback=validate_tablename_format,
)
@click.argument(
    "target_table",
    metavar="TARGET_TABLE",
    callback=validate_tablename_format,
)
@click.option(
    "--target-profile",
    "-tp",
    "target_profile_name",
    default=None,
    help="Name of the pre-configured profile for the target cluster.",
)
@click.option(
    "--target-hostname",
    "-h",
    default=None,
    help="Hostname of the target cluster.",
)
@click.option(
    "--target-username",
    "-u",
    default=None,
    help="Username for the target cluster.",
)
@click.option(
    "--target-password",
    "-p",
    default=None,
    help="Password for the target cluster.",
)
@click.option(
    "--target-uri-scheme",
    "-s",
    default=None,
    type=click.Choice(["http", "https"], case_sensitive=False),
    help="URI scheme for the target cluster (http or https).",
)
@click.option(
    "--allow-merge",
    type=bool,
    is_flag=True,
    is_eager=True,
    default=False,
    help="Allow migration even if the source table has the merge process enabled.",
)
@click.option(
    "--only",
    type=click.Choice(["resources", "data"], case_sensitive=False),
    help="Limit the migration to 'resources' (project, table, etc.) or 'data' (partitions).",
)
@click.option(
    "--with-functions",
    is_flag=True,
    default=False,
    help="Include functions in the resource migration.",
)
@click.option(
    "--with-dictionaries",
    is_flag=True,
    default=False,
    help="Include dictionaries in the resource migration.",
)
@click.option(
    "--from-date",
    cls=CustomDateTime,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    default=None,
    help="Minimum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.",
)
@click.option(
    "--to-date",
    cls=CustomDateTime,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    default=None,
    help="Maximum timestamp for filtering partitions in YYYY-MM-DD HH:MM:SS format.",
)
@click.option(
    "--reuse-partitions",
    type=bool,
    is_flag=True,
    default=False,
    help="Reuse existing data partitions instead of copying them. Requires shared storage.",
)
@click.option(
    "--rc-host",
    default=None,
    help="The hostname or IP address of the Rclone remote server.",
    callback=validate_hostname,
)
@click.option(
    "--rc-user",
    default=None,
    help="The username for authenticating with the Rclone remote server.",
)
@click.option(
    "--rc-pass",
    default=None,
    help="The password for authenticating with the Rclone remote server.",
)
@click.option(
    "--concurrency",
    default=20,
    type=click.IntRange(1, 50),
    help="Number of concurrent requests during file migration. Default to 20.",
    hidden=True,
)
@click.option(
    "--temp-catalog",
    type=bool,
    is_flag=True,
    default=False,
    help="Use a previously downloaded catalog stored in a temporary file, "
    "instead of downloading it again.",
    hidden=True,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def migrate(
    ctx: click.Context,
    source_table: str,
    target_table: str,
    target_profile_name: str,
    target_hostname: str,
    target_username: str,
    target_password: str,
    target_uri_scheme: str,
    allow_merge: bool,
    only: str,
    with_functions: bool,
    with_dictionaries: bool,
    from_date: datetime,
    to_date: datetime,
    reuse_partitions: bool,
    rc_host: str,
    rc_user: str,
    rc_pass: str,
    concurrency: int,
    temp_catalog: bool,
):
    """Migrate a table and its dependencies to a target cluster.

    \b
    This command orchestrates a table migration, which can involve two main stages:
    1. *Resource Creation*: Replicates the source project, table, and transforms
       on the target cluster. Optionally, it can also migrate associated
       functions and dictionaries.
    2. *Data Migration*: Copies the table's data from the source storage
       to the target and updates the catalog to make the data queryable.

    \b
    **Arguments**:
      - `SOURCE_TABLE`: The source table to migrate, in 'project.table' format.
      - `TARGET_TABLE`: The destination for the migration, in 'project.table' format.

    \b
    **Key Options**:
    - Target Cluster: Specify the destination with `--target-profile` or with individual
      connection details (`--target-hostname`, `--target-username`, etc.).
    \b
    - Migration Scope (`--only`):
      - *resources*: Migrates only the project, table, and other definitions.
      - *data*: Migrates only the data, assuming resources already exist.
      - If omitted, a full migration (resources and data) is performed.
    \b
    - Data Handling:
      - `--reuse-partitions`: For clusters sharing storage. Migrates the table
        definition but reuses the existing data, avoiding a data copy.
      - `--from-date`/`--to-date`: Filter the data to be migrated by a date range.
    \b
    - Rclone Remote:
      - `--rc-host`, `--rc-user`, `--rc-pass`: Connection details for the Rclone
        server that will perform the data transfer. Required for any migration
        that copies data.

    \b
    Examples:
      # Perform a full migration from a staging to a production project, including functions and dictionaries
      hdxcli --profile stage migrate staging_proj.logs prod_proj.logs --target-profile prod --rc-host rclone.host --rc-user rclone.user --rc-pass rclone.pass

    \b
      # Migrate only the resources (project, table, etc.), without copying data
      hdxcli --profile stage migrate staging_proj.logs prod_proj.logs --target-profile prod --only resources

    \b
      # Migrate only data for a specific date range, assuming resources already exist
      hdxcli --profile stage migrate staging_proj.logs prod_proj.logs --target-profile prod --only data --from-date "2025-01-01 00:00:00" --rc-host rclone.host --rc-user rclone.user --rc-pass rclone.pass

    \b
      # Perform a migration between clusters that share the same storage backend, avoiding data copy
      hdxcli --profile stage migrate staging_proj.logs prod_proj.logs --target-profile prod --reuse-partitions
    """
    source_profile = ctx.parent.obj["usercontext"]
    has_target_profile = target_profile_name is not None
    has_all_cluster_options = all([target_hostname, target_username, target_password, target_uri_scheme])

    if not has_target_profile and not has_all_cluster_options:
        raise click.BadParameter(
            "You must provide either --target-profile or all target connection options "
            "(--target-hostname, --target-username, --target-password, --target-uri-scheme)."
        )

    # Validate rclone parameters when data migration is needed
    if only != "resources" and not reuse_partitions and not all([rc_host, rc_user, rc_pass]):
        raise click.BadParameter(
            "The options --rc-host, --rc-user, and --rc-pass are required "
            "for migrations that include data transfer. Please provide them or use "
            "--only resources or --reuse-partitions."
        )

    target_profile = get_profile(
        target_profile_name,
        target_hostname,
        target_username,
        target_password,
        target_uri_scheme,
        source_profile.timeout,
    )

    if source_profile.hostname == target_profile.hostname and reuse_partitions:
        raise click.BadParameter(
            "--reuse-partitions must be used for migrations between different clusters."
        )

    logger.info(f"{' Resource Retrieval ':=^50}")
    source_resources = source_table.split(".")
    source_profile.projectname = source_resources[0]
    source_profile.tablename = source_resources[1]

    target_resources = target_table.split(".")
    target_profile.projectname = target_resources[0]
    target_profile.tablename = target_resources[1]

    source_data = MigrationData()
    target_data = MigrationData()
    rc_config = RcloneAPIConfig(rc_host, rc_user, rc_pass)

    # Source
    logger.info(f"Source Hostname: {source_profile.hostname}")
    get_resources(source_profile, source_data)
    catalog = None
    if only != "resources":
        catalog = get_catalog(source_profile, source_data, temp_catalog)
    logger.info("")

    # Target
    only_storages = only != "data"
    logger.info(f"Target Hostname: {target_profile.hostname}")
    get_resources(target_profile, target_data, only_storages=only_storages)
    logger.info("")

    validations(
        source_profile,
        source_data,
        target_data,
        catalog,
        from_date,
        to_date,
        only,
        allow_merge,
        reuse_partitions,
    )
    logger.info("")

    # Migrations
    # 'only' parameter has 3 possible values: 'resources', 'data', None
    # with these two if statements, it handles all the possible combinations
    if only != "data":
        create_resources(
            target_profile,
            target_data,
            source_profile,
            source_data,
            reuse_partitions,
            migrate_functions=with_functions,
            migrate_dictionaries=with_dictionaries
        )
    if only != "resources":
        migrate_data(
            source_profile,
            source_data.storages,
            target_profile,
            target_data,
            catalog,
            rc_config,
            concurrency,
            reuse_partitions,
        )

    logger.info(f"{' Migration Completed ':=^50}")
