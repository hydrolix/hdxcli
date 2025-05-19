import json
from urllib.parse import urlparse

from ....auth.context_builder import get_profile
from ....library_api.common.exceptions import HdxCliException, HttpException
from ....library_api.common.generic_resource import access_resource_detailed
from ....library_api.common.logging import get_logger
from ....models import ProfileUserContext
from ...common.undecorated_click_commands import basic_show
from ..undecorated_click_commands import basic_create, basic_create_file, basic_get
from .logs import LogType, log_message, log_migration_status
from .migration_rollback import (
    DoNothingMigrationRollbackManager,
    MigrateStatus,
    MigrationRollbackManager,
    ResourceKind,
)
from .resource_adapters import (
    normalize_dictionary,
    normalize_function,
    normalize_project,
    normalize_table,
    normalize_transform,
)

logger = get_logger()


def migrate_resource_config(
    resource_type: str,
    source_profile: ProfileUserContext,
    target_profile_name: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
    **kwargs,
):
    """
    Main method to migrate different types of resources.
    It performs a login profile if needed,
    and then calls the specific function based on the resource type.

    Args:
        resource_type (str): The type of resource to migrate. Supported values include:
            - "project": To migrate projects.
            - "table": To migrate tables.
            - "transform": To migrate transforms.
            - "dictionary": To migrate dictionaries.
            - "function": To migrate functions.
        source_profile (ProfileUserContext): The source profile context containing the connection
            details and configuration for the source cluster.
        target_profile_name (str): The name of the target profile for migration.
        target_cluster_hostname (str): The hostname of the target cluster.
        target_cluster_username (str): The username for authenticating with the target cluster.
        target_cluster_password (str): The password for authenticating with the target cluster.
        target_cluster_uri_scheme (str): The URI scheme for the target cluster (e.g., "http" or "https").
        no_rollback (bool): If True, the migration will not be rolled back in case of an error.
        **kwargs: Additional arguments specific to the resource being migrated. These may include
            resource-specific configurations or settings.

    Raises:
        HdxCliException: If the resource type is unknown or unsupported.
    """
    resource_migration_methods = {
        "project": migrate_projects,
        "table": migrate_tables,
        "transform": migrate_transforms,
        "dictionary": migrate_dictionaries,
        "function": migrate_functions,
    }
    migration_method = resource_migration_methods.get(resource_type)

    if not migration_method:
        raise HdxCliException(f"Unknown or unsupported migration resource type: {resource_type}")

    target_profile = get_profile(
        target_profile_name,
        target_cluster_hostname,
        target_cluster_username,
        target_cluster_password,
        target_cluster_uri_scheme,
        source_profile.timeout,
    )
    # Make sure the target profile has the same timeout as the source profile
    target_profile.timeout = source_profile.timeout

    mrm = MigrationRollbackManager
    if no_rollback:
        mrm = DoNothingMigrationRollbackManager
    with mrm(target_profile) as migration_rollback_manager:
        # Call the specific migration function with the provided arguments
        migration_method(
            source_profile=source_profile,
            target_profile=target_profile,
            rollback_manager=migration_rollback_manager,
            **kwargs,
        )


def migrate_projects(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_project: str,
    target_project: str,
    only: bool = False,
    dicts: bool = False,
    functs: bool = False,
):
    _, source_projects_url = access_resource_detailed(source_profile, [("projects", None)])
    _, target_projects_url = access_resource_detailed(target_profile, [("projects", None)])
    source_projects_path = urlparse(source_projects_url).path
    target_projects_path = urlparse(target_projects_url).path

    project_name, status = migrate_project(
        source_profile,
        target_profile,
        source_projects_path,
        target_projects_path,
        source_project,
        target_project,
    )
    log_migration_status("Project", project_name, status, rollback_manager, ResourceKind.PROJECT)

    if dicts:
        migrate_dictionaries(
            source_profile, target_profile, rollback_manager, source_project, target_project
        )
    if functs:
        migrate_functions(
            source_profile, target_profile, rollback_manager, source_project, target_project
        )

    if only:
        return

    migrate_tables(
        source_profile, target_profile, rollback_manager, source_project, target_project, only=only
    )


def migrate_project(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    source_projects_path: str,
    target_projects_path: str,
    source_project: str,
    target_project: str,
):
    """Migrates a single project."""
    log_message(LogType.INFO, f"Migrating project '{target_project}'...", indent=0)
    project_settings = json.loads(basic_show(source_profile, source_projects_path, source_project))
    try:
        project_settings["name"] = target_project
        project_settings = normalize_project(project_settings)

        basic_create(target_profile, target_projects_path, body=project_settings)
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            logger.debug(f"Error migrating project: {exc}")
            raise exc

        logger.debug(f"Project already exists: {exc}")
        return target_project, MigrateStatus.SKIPPED

    return target_project, MigrateStatus.CREATED


def migrate_tables(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_project: str,
    target_project: str,
    *,
    source_table: str = None,
    target_table: str = None,
    only: bool = False,
):
    tables, source_tables_url = access_resource_detailed(
        source_profile, [("projects", source_project), ("tables", None)]
    )
    _, target_tables_url = access_resource_detailed(
        target_profile, [("projects", target_project), ("tables", None)]
    )
    source_tables_path = urlparse(source_tables_url).path
    target_tables_path = urlparse(target_tables_url).path

    if source_table and target_table:
        source_table_info = next((tbl for tbl in tables if tbl["name"] == source_table), {})
        # If summary type, there won't be transform to be migrated
        summary = source_table_info.get("type") == "summary"

        _migrate_single_table_with_optionals(
            source_profile,
            target_profile,
            rollback_manager,
            source_tables_path,
            target_tables_path,
            source_project,
            target_project,
            source_table,
            target_table,
            only or summary,
        )
        return

    tables_sorted = sorted(tables, key=lambda tbl: tbl["type"], reverse=True)

    for table in tables_sorted:
        table_name = table.get("name")
        # If summary type, there won't be transform to be migrated
        summary = table.get("type") == "summary"

        _migrate_single_table_with_optionals(
            source_profile,
            target_profile,
            rollback_manager,
            source_tables_path,
            target_tables_path,
            source_project,
            target_project,
            table_name,
            table_name,
            only or summary,
        )


def _migrate_single_table_with_optionals(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_tables_path: str,
    target_tables_path: str,
    source_project: str,
    target_project: str,
    source_table: str,
    target_table: str,
    only: bool,
):
    _, status = migrate_table(
        source_profile,
        target_profile,
        source_tables_path,
        target_tables_path,
        source_table,
        target_table,
    )
    log_migration_status(
        "Table", target_table, status, rollback_manager, ResourceKind.TABLE, [target_project]
    )

    if only:
        return

    migrate_transforms(
        source_profile,
        target_profile,
        rollback_manager,
        source_project,
        target_project,
        source_table,
        target_table,
    )


def migrate_table(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    source_tables_path: str,
    target_tables_path: str,
    source_table: str,
    target_table: str,
):
    """Migrates a single table."""
    log_message(LogType.INFO, f"Migrating table '{target_table}'...", indent=0)
    table_settings = json.loads(basic_show(source_profile, source_tables_path, source_table))
    try:
        table_settings["name"] = target_table
        table_settings = normalize_table(table_settings)

        basic_create(target_profile, target_tables_path, body=table_settings)
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            logger.debug(f"Error migrating table: {exc}")
            raise

        logger.debug(f"Table already exists: {exc}")
        return target_table, MigrateStatus.SKIPPED

    return target_table, MigrateStatus.CREATED


def migrate_transforms(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_project: str,
    target_project: str,
    source_table: str,
    target_table: str,
    *,
    source_transform: str = None,
    target_transform: str = None,
):
    transforms, source_transforms_url = access_resource_detailed(
        source_profile,
        [("projects", source_project), ("tables", source_table), ("transforms", None)],
    )
    _, target_transforms_url = access_resource_detailed(
        target_profile,
        [("projects", target_project), ("tables", target_table), ("transforms", None)],
    )
    source_transforms_path = urlparse(source_transforms_url).path
    target_transforms_path = urlparse(target_transforms_url).path

    if source_transform and target_transform:
        _, status = migrate_transform(
            source_profile,
            target_profile,
            source_transforms_path,
            target_transforms_path,
            source_transform,
            target_transform,
        )
        log_migration_status(
            "Transform",
            target_transform,
            status,
            rollback_manager,
            ResourceKind.TRANSFORM,
            [target_project, target_table],
        )
        return

    for transform in transforms:
        transform_name = transform.get("name")
        _, status = migrate_transform(
            source_profile,
            target_profile,
            source_transforms_path,
            target_transforms_path,
            transform_name,
            transform_name,
        )
        log_migration_status(
            "Transform",
            transform_name,
            status,
            rollback_manager,
            ResourceKind.TRANSFORM,
            [target_project, target_table],
        )


def migrate_transform(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    source_transforms_path: str,
    target_transforms_path: str,
    source_transform: str,
    target_transform: str,
):
    """Migrates a single transform."""
    log_message(LogType.INFO, f"Migrating transform '{target_transform}'...", indent=0)
    transform_settings = json.loads(
        basic_show(source_profile, source_transforms_path, source_transform)
    )
    try:
        transform_settings["name"] = target_transform
        transform_settings = normalize_transform(transform_settings)

        basic_create(target_profile, target_transforms_path, body=transform_settings)
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            logger.debug(f"Error migrating transform: {exc}")
            raise exc

        logger.debug(f"Transform already exists: {exc}")
        return target_transform, MigrateStatus.SKIPPED

    return target_transform, MigrateStatus.CREATED


def migrate_dictionaries(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_project: str,
    target_project: str,
    *,
    source_dictionary: str = None,
    target_dictionary: str = None,
):
    dicts, source_dicts_url = access_resource_detailed(
        source_profile, [("projects", source_project), ("dictionaries", None)]
    )
    _, target_dicts_url = access_resource_detailed(
        target_profile, [("projects", target_project), ("dictionaries", None)]
    )
    source_dicts_path = urlparse(source_dicts_url).path
    target_dicts_path = urlparse(target_dicts_url).path

    if source_dictionary and target_dictionary:
        _, status = migrate_dictionary(
            source_profile,
            target_profile,
            source_dicts_path,
            target_dicts_path,
            source_project,
            target_project,
            source_dictionary,
            target_dictionary,
        )
        log_migration_status(
            "Dictionary",
            target_dictionary,
            status,
            rollback_manager,
            ResourceKind.DICTIONARY,
            [target_project],
        )
        return

    for dictionary in dicts:
        dictionary_name = dictionary.get("name")
        _, status = migrate_dictionary(
            source_profile,
            target_profile,
            source_dicts_path,
            target_dicts_path,
            source_project,
            target_project,
            dictionary_name,
            dictionary_name,
        )
        log_migration_status(
            "Dictionary",
            dictionary_name,
            status,
            rollback_manager,
            ResourceKind.DICTIONARY,
            [target_project],
        )


def migrate_dictionary(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    source_dicts_path: str,
    target_dicts_path: str,
    source_project: str,
    target_project: str,
    source_dict: str,
    target_dict: str,
):
    """Migrates a single dictionary."""
    log_message(LogType.INFO, f"Migrating dictionary '{target_dict}'...", indent=0)
    dictionary = json.loads(basic_show(source_profile, source_dicts_path, source_dict))

    d_settings = dictionary["settings"]
    d_name = dictionary["name"]
    d_file = d_settings["filename"]
    d_format = d_settings["format"]
    table_name = f"{source_project}_{d_name}"

    query_path = "/query/"
    query_sql = f"SELECT * FROM {table_name} FORMAT {d_format}"
    dict_file_content = basic_get(source_profile, query_path, fmt="verbatim", query=query_sql)

    try:
        _create_dictionary_file_for_project(
            target_profile, target_dicts_path, d_file, dict_file_content
        )
    except HttpException as exc:
        if exc.error_code != 400:
            logger.debug(f"Error migrating dictionary file for project: {exc}")
            raise
        logger.debug(f"Dictionary file {d_file} already exists ({exc}). Skipping.")

    try:
        dictionary["name"] = target_dict
        dictionary = normalize_dictionary(dictionary)

        basic_create(target_profile, target_dicts_path, body=dictionary)
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            logger.debug(f"Error migrating dictionary: {exc}")
            raise exc

        logger.debug(f"Dictionary already exists: {exc}")
        return target_dict, MigrateStatus.SKIPPED

    return target_dict, MigrateStatus.CREATED


def _create_dictionary_file_for_project(
    profile: ProfileUserContext, target_dicts_path: str, dict_file: str, dict_file_contents: bytes
):
    """Migrates a single dictionary file."""
    target_dict_files_path = f"{target_dicts_path}files/"
    basic_create_file(
        profile,
        target_dict_files_path,
        dict_file,
        file_content=dict_file_contents,
    )


def migrate_functions(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    rollback_manager: MigrationRollbackManager | DoNothingMigrationRollbackManager,
    source_project: str,
    target_project: str,
    *,
    source_function: str = None,
    target_function: str = None,
):
    functs, source_functs_url = access_resource_detailed(
        source_profile, [("projects", source_project), ("functions", None)]
    )
    _, target_functs_url = access_resource_detailed(
        target_profile, [("projects", target_project), ("functions", None)]
    )
    source_functs_path = urlparse(source_functs_url).path
    target_functs_path = urlparse(target_functs_url).path

    if source_function and target_function:
        _, status = migrate_function(
            source_profile,
            target_profile,
            source_functs_path,
            target_functs_path,
            source_function,
            target_function,
        )
        log_migration_status(
            "Function",
            target_function,
            status,
            rollback_manager,
            ResourceKind.FUNCTION,
            [target_project],
        )
        return

    for function in functs:
        funct_name = function.get("name")
        _, status = migrate_function(
            source_profile,
            target_profile,
            source_functs_path,
            target_functs_path,
            funct_name,
            funct_name,
        )
        log_migration_status(
            "Function",
            funct_name,
            status,
            rollback_manager,
            ResourceKind.FUNCTION,
            [target_project],
        )


def migrate_function(
    source_profile: ProfileUserContext,
    target_profile: ProfileUserContext,
    source_functs_path: str,
    target_functs_path: str,
    source_function: str,
    target_function: str,
):
    """Migrates a single function."""
    log_message(LogType.INFO, f"Migrating function '{target_function}'...", indent=0)
    function_settings = json.loads(basic_show(source_profile, source_functs_path, source_function))
    try:
        function_settings["name"] = target_function
        function_settings = normalize_function(function_settings)

        basic_create(target_profile, target_functs_path, body=function_settings)
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            logger.debug(f"Error migrating function: {exc}")
            raise exc

        logger.debug(f"Function already exists: {exc}")
        return target_function, MigrateStatus.SKIPPED

    return target_function, MigrateStatus.CREATED
