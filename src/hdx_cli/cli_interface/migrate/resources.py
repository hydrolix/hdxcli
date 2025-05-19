import copy
from urllib.parse import urlparse

from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_create,
    basic_create_file,
    basic_get,
)
from hdx_cli.cli_interface.migrate.helpers import MigrationData, confirm_action
from hdx_cli.cli_interface.migrate.resource_adapter import (
    adapt_resource_to_api_structure,
    normalize_dictionary,
    normalize_function,
    normalize_project,
    normalize_table,
    normalize_transform,
)
from hdx_cli.library_api.common.exceptions import (
    HdxCliException,
    HttpException,
    ResourceNotFoundException,
    StorageNotFoundError,
)
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import (
    get_equivalent_storages,
    get_storage_by_id,
    get_storage_default,
    valid_storage_id,
)
from hdx_cli.models import ProfileUserContext

logger = get_logger()


def create_resources(
    target_profile: ProfileUserContext,
    target_data: MigrationData,
    source_profile: ProfileUserContext,
    source_data: MigrationData,
    reuse_partitions: bool = False,
) -> None:
    logger.info(f'{" Resource Creation ":=^50}')
    logger.info(f"Target Cluster: {target_profile.hostname}")

    # PROJECT
    _create_project(target_profile, source_data.project, reuse_partitions)
    target_data.project, _ = access_resource_detailed(
        target_profile, [("projects", target_profile.projectname)]
    )
    if reuse_partitions and source_data.project.get("uuid") != target_data.project.get("uuid"):
        raise HdxCliException("The source and target resources must have the same UUID.")

    # FUNCTIONS
    if source_data.functions:
        _create_functions(target_profile, source_data.functions)

    # DICTIONARIES
    if source_data.dictionaries:
        _create_dictionaries(target_profile, source_profile, source_data.dictionaries)

    # TABLE
    _create_table(target_profile, source_data.table, reuse_partitions)
    target_data.table, _ = access_resource_detailed(
        target_profile,
        [("projects", target_profile.projectname), ("tables", target_profile.tablename)],
    )

    # If table type is summary, then there are no transforms to create
    if source_data.transforms and source_data.table.get("type") != "summary":
        # TRANSFORMS
        _create_transforms(target_profile, source_data.transforms)

    logger.info("")


def _create_project(
    target_profile: ProfileUserContext, source_project_body: dict, reuse_partitions: bool
) -> None:
    logger.info(f"{f'  Project: {target_profile.projectname[:31]}':<42} -> [!n]")

    _, target_projects_url = access_resource_detailed(target_profile, [("projects", None)])
    target_projects_path = urlparse(target_projects_url).path
    target_project_body = copy.deepcopy(source_project_body)

    adapted_project = adapt_resource_to_api_structure(
        target_profile, target_projects_path, target_project_body
    )
    normalized_project = normalize_project(adapted_project, reuse_partitions)

    try:
        basic_create(
            target_profile,
            target_projects_path,
            target_profile.projectname,
            body=normalized_project,
        )
        logger.info("Done")
    except HttpException as exc:
        if exc.error_code != 400 or "already exists" not in str(exc.message):
            raise exc
        logger.info("Exists, skipping")


def _create_functions(
    target_profile: ProfileUserContext,
    source_function_list: list,
) -> None:
    logger.info(f"{f'  Functions':<42} -> In progress")
    _, target_functions_url = access_resource_detailed(
        target_profile, [("projects", target_profile.projectname), ("functions", None)]
    )
    target_functions_path = urlparse(target_functions_url).path

    for function in source_function_list:
        function_name = function.get("name")
        logger.info(f"{f'    name: {function_name}':<42} -> [!n]")

        adapted_function = adapt_resource_to_api_structure(
            target_profile, target_functions_path, function
        )
        normalized_function = normalize_function(adapted_function)
        message = None
        try:
            basic_create(
                target_profile, target_functions_path, function_name, body=normalized_function
            )
            message = "Done"
        except HttpException as exc:
            if exc.error_code != 400 or "already exists" not in str(exc.message):
                logger.debug(f"Error creating function '{function_name}': {exc}")
                message = "Done with errors"
            else:
                logger.debug(f"Function '{function_name}' already exists, skipping")
                message = "Exists, skipping"
        finally:
            logger.info(message)


def _create_dictionaries(
    target_profile: ProfileUserContext,
    source_profile: ProfileUserContext,
    source_dictionary_list: list,
) -> None:
    logger.info(f"{f'  Dictionaries':<42} -> In progress")
    _, target_dictionaries_url = access_resource_detailed(
        target_profile, [("projects", target_profile.projectname), ("dictionaries", None)]
    )
    target_dictionaries_path = urlparse(target_dictionaries_url).path

    dictionary_files_so_far = set()
    for dictionary in source_dictionary_list:
        d_name = dictionary.get("name")
        logger.info(f"{f'    name: {d_name}':<42} -> [!n]")
        adapted_dictionary = adapt_resource_to_api_structure(
            target_profile, target_dictionaries_path, dictionary
        )
        normalized_dictionary = normalize_dictionary(adapted_dictionary)

        d_settings = normalized_dictionary.get("settings")
        d_file = d_settings.get("filename")
        d_format = d_settings.get("format")
        table_name = f"{source_profile.projectname}_{d_name}"
        query_path = "/query/"
        query_sql = f"SELECT * FROM {table_name} FORMAT {d_format}"
        dict_file_content = basic_get(source_profile, query_path, fmt="verbatim", query=query_sql)

        try:
            if d_file not in dictionary_files_so_far:
                _create_dictionary_file(
                    target_profile, target_dictionaries_path, d_file, dict_file_content
                )
                dictionary_files_so_far.add(d_file)
        except HttpException as exc:
            if exc.error_code != 400 or "already exists" not in str(exc.message):
                logger.debug(f"Error creating dictionary file '{d_file}': {exc}")
            else:
                logger.debug(f"Dictionary file '{d_file}' already exists, skipping")
        except Exception as exc:
            logger.debug(f"Unexpected error creating dictionary file '{d_file}': {exc}")
        finally:
            message = None
            try:
                basic_create(
                    target_profile, target_dictionaries_path, d_name, body=normalized_dictionary
                )
                message = "Done"
            except HttpException as exc:
                if exc.error_code != 400 or "already exists" not in str(exc.message):
                    logger.debug(f"Error creating dictionary file '{d_file}': {exc}")
                    message = "Done with errors"
                else:
                    logger.debug(f"Dictionary '{d_name}' already exists, skipping")
                    message = "Exists, skipping"
            except Exception as exc:
                logger.debug(f"Unexpected error creating dictionary '{d_name}': {exc}")
                message = "Done with errors"
            finally:
                logger.info(message)


def _create_dictionary_file(
    profile: ProfileUserContext, target_dicts_path: str, dict_file: str, dict_file_contents: bytes
) -> None:
    target_dict_files_path = f"{target_dicts_path}files/"
    basic_create_file(
        profile,
        target_dict_files_path,
        dict_file,
        file_content=dict_file_contents,
    )


def _create_table(
    target_profile: ProfileUserContext, source_table_body: dict, reuse_partitions: bool
) -> None:
    logger.info(f"{f'  Table: {target_profile.tablename[:33]}':<42} -> [!n]")

    _, target_table_url = access_resource_detailed(
        target_profile, [("projects", target_profile.projectname), ("tables", None)]
    )
    target_tables_path = urlparse(target_table_url).path
    target_table_body = copy.deepcopy(source_table_body)

    adapted_table = adapt_resource_to_api_structure(
        target_profile, target_tables_path, target_table_body
    )
    normalized_table = normalize_table(adapted_table, reuse_partitions)

    basic_create(
        target_profile, target_tables_path, target_profile.tablename, body=normalized_table
    )
    logger.info("Done")


def _create_transforms(
    target_profile: ProfileUserContext,
    source_transform_list: list,
) -> None:
    logger.info(f"{f'  Transforms':<42} -> In progress")

    _, target_transforms_url = access_resource_detailed(
        target_profile,
        [
            ("projects", target_profile.projectname),
            ("tables", target_profile.tablename),
            ("transforms", None),
        ],
    )
    target_transforms_path = urlparse(target_transforms_url).path

    for transform in source_transform_list:
        transform_name = transform.get("name")
        logger.info(f"{f'    name: {transform_name}':<42} -> [!n]")
        adapted_transform = adapt_resource_to_api_structure(
            target_profile, target_transforms_path, transform
        )
        normalized_transform = normalize_transform(adapted_transform)

        basic_create(
            target_profile, target_transforms_path, transform_name, body=normalized_transform
        )
        logger.info("Done")


def get_resources(
    profile: ProfileUserContext, data: MigrationData, only_storages: bool = False
) -> None:
    if not only_storages:
        logger.info(f"{f'  Project: {profile.projectname[:31]}':<42} -> [!n]")
        data.project, _ = access_resource_detailed(profile, [("projects", profile.projectname)])
        if not data.project:
            raise ResourceNotFoundException(f"The project '{profile.projectname}' was not found.")
        logger.info("Done")

        logger.info(f"{f'  Functions':<42} -> [!n]")
        data.functions, _ = access_resource_detailed(
            profile, [("projects", profile.projectname), ("functions", None)]
        )
        logger.info("Done")

        logger.info(f"{f'  Dictionaries':<42} -> [!n]")
        data.dictionaries, _ = access_resource_detailed(
            profile, [("projects", profile.projectname), ("dictionaries", None)]
        )
        logger.info("Done")

        logger.info(f"{f'  Table: {profile.tablename[:33]}':<42} -> [!n]")
        data.table, _ = access_resource_detailed(
            profile, [("projects", profile.projectname), ("tables", profile.tablename)]
        )
        if not data.table:
            raise ResourceNotFoundException(f"The table '{profile.tablename}' was not found.")
        logger.info("Done")

        logger.info(f"{'  Transforms':<42} -> [!n]")
        data.transforms, _ = access_resource_detailed(
            profile,
            [
                ("projects", profile.projectname),
                ("tables", profile.tablename),
                ("transforms", None),
            ],
        )
        if not data.transforms:
            raise ResourceNotFoundException(
                f"Transforms in the table '{profile.tablename}' were not found."
            )
        logger.info("Done")

    logger.info(f"{'  Storages':<42} -> [!n]")
    data.storages, _ = access_resource_detailed(profile, [("storages", None)])
    logger.info("Done")


def update_equivalent_multi_storage_settings(
    source_data: MigrationData, target_storages: list[dict]
) -> None:
    storage_equivalences = get_equivalent_storages(source_data.storages, target_storages)
    table_body = source_data.table
    storage_map = table_body.get("settings").get("storage_map")
    default_storage_id = storage_map.get("default_storage_id")
    if not (new_default_storage_id := storage_equivalences.get(default_storage_id)):
        raise HdxCliException(
            f"Storage ID '{default_storage_id}' not found in the target storages."
        )

    storage_map["default_storage_id"] = new_default_storage_id

    if mapping := storage_map.get("column_value_mapping"):
        new_mapping = {}
        for _, values in mapping.items():
            if not (new_storage := storage_equivalences.get(default_storage_id)):
                raise HdxCliException(
                    f"Storage ID '{default_storage_id}' not found in the target storages."
                )
            new_mapping[new_storage] = values
        storage_map["column_value_mapping"] = new_mapping


def interactive_set_default_storage(table_body: dict, target_storages: list[dict]) -> None:
    logger.info("In progress")
    logger.info("")
    default_storage_id, _ = get_storage_default(target_storages)

    header = " Storage Settings "
    logger.info(f"{header:*^40}")
    logger.info("* Specify the storage UUID for the new table, or")
    logger.info("* press Enter to use the default storage.")
    logger.info("*")

    for attempt in range(3):
        remaining_attempts = 2 - attempt
        logger.info(f"* Default storage UUID ({default_storage_id}): [!i]")
        user_input = input().strip().lower()

        if not user_input or valid_storage_id(user_input, target_storages):
            default_storage_id = user_input if user_input else default_storage_id
            _, storage = get_storage_by_id(target_storages, default_storage_id)
            storage_name = storage.get("name")
            storage_settings = storage.get("settings")

            if storage_name and storage_settings:
                logger.info(f"*  Storage Name: {storage_name}")
                for key, value in storage_settings.items():
                    logger.info(f"*   {key}: {value}")
                logger.info("*")
                if confirm_action(prompt="* Set this as the default storage for the table?"):
                    break
        else:
            logger.info(
                f"* Invalid storage UUID. Please try again. Attempts left: {remaining_attempts}"
            )
    else:
        raise StorageNotFoundError(
            "Attempt limit reached. Storage UUID not found in the target cluster."
        )

    table_body["settings"]["storage_map"] = {"default_storage_id": default_storage_id}
    logger.info(f'{"*" * 40:<42} -> [!n]')
