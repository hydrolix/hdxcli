import re

from ....library_api.common.logging import get_logger
from .logs import LogType, log_message, log_without_type

logger = get_logger()


def prompt_user_for_value(key: str, message=None, default=None):
    if not message:
        default_message = f" (default: {default})" if default else ""
        prompt_message = f"Please, provide a value for '{key}'{default_message}"
    else:
        prompt_message = message
    prompt_message = f"{prompt_message}: [!i]"

    log_without_type(prompt_message)
    user_input = input().strip()
    return user_input if user_input else default


def normalize_project(project: dict, reuse_partitions: bool = False) -> dict:
    if not reuse_partitions:
        project.pop("uuid", None)

    return project


def normalize_table(table: dict, reuse_partitions: bool = False) -> dict:
    table_name = table.get("name")
    if not reuse_partitions:
        table.pop("uuid", None)
    table_settings = table.get("settings", {})

    # Summary
    if table.get("type") == "summary":
        summary_settings = table_settings.get("summary", {})
        table_settings["summary"] = _update_parent_in_summary_sql(table_name, summary_settings)

    # Autoingest
    autoingest = table_settings.get("autoingest")
    filtered_autoingest = []
    if autoingest and isinstance(autoingest, list):
        for element in autoingest:
            if isinstance(element, dict) and element.get("enabled"):
                updated_element = _adapt_table_autoingest(table_name, element)
                if updated_element:
                    filtered_autoingest.append(updated_element)
    if filtered_autoingest:
        table_settings["autoingest"] = filtered_autoingest
    else:
        table_settings.pop("autoingest", None)

    # Merge pools
    merge = table_settings.get("merge")
    if merge and isinstance(merge, dict) and (pools := merge.get("pools")):
        updated_pools = _adapt_table_merge_pools(table_name, pools)
        if updated_pools:
            merge["pools"] = updated_pools
        else:
            merge.pop("pools", None)

    # Storage settings
    storage_map = table_settings.get("storage_map", {})
    if storage_map and storage_map.get("default_storage_id"):
        updated_storage_map = _adapt_table_storage_map(table_name, storage_map)
        if updated_storage_map:
            table_settings["storage_map"] = updated_storage_map
        else:
            table_settings.pop("storage_map", None)

    return table


def _update_parent_in_summary_sql(table_name: str, summary_settings: dict) -> dict:
    sql = summary_settings.get("sql")
    summary_sql = summary_settings.get("summary_sql")
    if not sql:
        log_message(LogType.WARNING, "No SQL was found in the summary settings.")
        log_message(LogType.PROMPT, "Please provide a valid SQL statement for the summary table")
        sql = get_user_value_input("sql", "string")
        logger.info("")

    log_message(LogType.WARNING, f"Summary settings found in the table '{table_name}'")
    current_project, current_table = _extract_table_name_from_sql(sql)
    current_summary_parents = (
        "Not Found"
        if not current_project or not current_table
        else f"{current_project}.{current_table}"
    )
    log_without_type(f"The current parents for the summary table are: {current_summary_parents}")
    logger.info("")

    log_message(
        LogType.PROMPT,
        "Please enter a new project and table in 'project.table' format "
        "(leave blank to keep current)",
    )
    attempts = 3
    while attempts > 0:
        log_without_type("New parents 'project.table': [!i]")
        new_project_table = input().strip()

        if not new_project_table and current_project and current_table:
            new_project, new_table = current_project, current_table
            break

        try:
            new_project, new_table = new_project_table.split(".")
            if new_project and new_table:
                break
            log_without_type("Invalid input. Ensure 'project.table' format is used")
        except ValueError:
            log_without_type("Invalid format. Please enter 'project.table' with a single dot")
            attempts -= 1

    if attempts == 0:
        log_without_type("Maximum attempts reached. Keeping current project.table")
        new_project, new_table = current_project, current_table

    summary_settings["sql"] = sql.replace(current_project, new_project).replace(
        current_table, new_table
    )
    if summary_sql:
        summary_settings["summary_sql"] = summary_sql.replace(current_project, new_project).replace(
            current_table, new_table
        )

    logger.info("")
    return summary_settings


def _extract_table_name_from_sql(sql):
    pattern = r"(?i)\bFROM\s+([`\"']?\w+[`\"']?\.\w+)"
    match = re.search(pattern, sql)
    if not match:
        return None, None

    table_name = match.group(1).replace("`", "").replace('"', "").replace("'", "")
    project, table = table_name.split(".")
    return project, table


def _adapt_table_storage_map(table_name: str, storage_map: dict) -> dict | None:
    default_storage_id = storage_map.get("default_storage_id")
    column_name = storage_map.get("column_name", "-")
    column_value_mapping = storage_map.get("column_value_mapping", "-")

    log_message(LogType.WARNING, f"Storage settings found in the table '{table_name}'")
    log_without_type(
        [
            f"Default Storage ID: {default_storage_id}",
            f"Column Name: {column_name}",
            f"Column Value Mapping: {column_value_mapping}",
            "",
        ]
    )
    log_message(LogType.PROMPT, "How would you like to proceed?")
    log_without_type(
        [
            "1) Preserve all existing settings without any changes",
            "2) Specify a new default storage ID",
            "3) Remove the storage settings (use cluster default)",
        ]
    )

    user_input = 4
    while user_input not in ["1", "2", "3"]:
        log_without_type("Please enter your choice (1/2/3): [!i]")
        user_input = input().strip().lower()

    if user_input == "1":
        logger.debug("Preserving the existing storage settings.")
    elif user_input == "2":
        new_default_storage_id = prompt_user_for_value(
            "default_storage_id",
            message="Please enter the new default storage ID",
            default=default_storage_id,
        )
        storage_map = {"default_storage_id": new_default_storage_id}
    elif user_input == "3":
        storage_map = None

    logger.info("")
    return storage_map


def _adapt_table_merge_pools(table_name: str, pools: dict) -> dict | None:
    log_message(LogType.WARNING, f"Merge pools settings found in the table '{table_name}'")
    for key, value in pools.items():
        log_without_type(f"{key}: {value}")
    logger.info("")

    log_message(LogType.PROMPT, "How would you like to proceed?")
    log_without_type(
        [
            "1) Preserve all existing settings without any changes",
            "2) Specify a new merge pools settings",
            "3) Remove the merge pools settings",
        ]
    )
    user_input = 4
    while user_input not in ["1", "2", "3"]:
        log_without_type("Please enter your choice (1/2/3): [!i]")
        user_input = input().strip().lower()

    updated_pools = {}
    if user_input == "1":
        logger.debug("Preserving the existing merge pools settings.")
        updated_pools = pools
    elif user_input == "2":
        for key in pools.keys():
            new_value = prompt_user_for_value(key)
            if new_value:
                updated_pools[key] = new_value
    elif user_input == "3":
        updated_pools = None

    logger.info("")
    return updated_pools


def _adapt_table_autoingest(table_name: str, autoingest: dict) -> dict | None:
    log_message(LogType.WARNING, f"Autoingest settings found in the table '{table_name}'")
    for key, value in autoingest.items():
        log_without_type(f"{key}: {value}")

    log_message(LogType.PROMPT, "How would you like to proceed?")
    log_without_type(
        [
            "1) Preserve all existing settings without any changes",
            "2) Specify a new autoingest settings",
            "3) Remove the autoingest settings",
        ]
    )

    user_input = 4
    while user_input not in ["1", "2", "3"]:
        log_without_type("Please enter your choice (1/2/3): [!i]")
        user_input = input().strip().lower()

    updated_autoingest = {}
    if user_input == "1":
        logger.debug("Preserving the existing autoingest settings.")
        updated_autoingest = autoingest
    elif user_input == "2":
        for key in ["transform", "source_credential_id", "bucket_credential_id"]:
            new_value = prompt_user_for_value(key)
            if new_value:
                updated_autoingest[key] = new_value
    elif user_input == "3":
        updated_autoingest = None

    logger.info("")
    return updated_autoingest


def normalize_transform(transform: dict) -> dict:
    # Always remove the UUID
    transform.pop("uuid", None)

    # If sample_data is a list, normalize it to a single json or csv format
    settings = transform.get("settings", {})
    sample_data = settings.get("sample_data", None)
    if isinstance(sample_data, list) and len(sample_data) == 1:
        settings["sample_data"] = sample_data[0]

    return transform


def normalize_function(function: dict) -> dict:
    # Always remove the UUID
    function.pop("uuid", None)
    return function


def normalize_dictionary(dictionary: dict) -> dict:
    # Always remove the UUID
    dictionary.pop("uuid", None)
    return dictionary


def get_user_value_input(field_name: str, field_type: str):
    while True:
        user_input = prompt_user_for_value(field_name, message=f"* {field_name} ({field_type})")

        if not user_input:
            log_without_type("Invalid value. Please, try again")
            continue

        if "integer" in field_type:
            try:
                return int(user_input)
            except (ValueError, TypeError):
                log_without_type("Invalid integer. Please enter a valid number")
        elif "list" in field_type:
            return [item.strip() for item in user_input.split(",") if item.strip()]
        elif "boolean" in field_type:
            if user_input.lower() in ("1", "true", "t", "yes", "y"):
                return True
            if user_input.lower() in ("0", "false", "f", "no", "n"):
                return False
            log_without_type("Invalid value. Please enter 'true' or 'false'")
        elif "decimal" in field_type:
            try:
                return float(user_input)
            except (ValueError, TypeError):
                log_without_type("Invalid value. Please enter a valid decimal number")
        else:
            return user_input


# def _adapt_resource_to_api_structure(
#         resource_structure: dict,
#         resource_settings: dict | None,
#         parent_path="",
#         first_time_input=True
# ) -> dict:
#     def is_empty(value):
#         return value in (None, {}, [], "")
#
#     adapted_resource_settings = {}
#     for field_name, field_props in resource_structure.items():
#         if field_props.get("read_only", False):
#             continue
#
#         is_required = field_props.get("required", False)
#         field_type = field_props.get("type", "")
#         resource_settings_value = resource_settings.get(field_name) if resource_settings else None
#
#         current_path = f"{parent_path}.{field_name}" if parent_path else field_name
#
#         if field_type == "nested object":
#             children_structure = field_props.get("children", {})
#             child_structure = field_props.get("child", None)
#
#             if children_structure:
#                 if not resource_settings_value and not is_required:
#                     continue
#                 nested_dict, first_time_input = _adapt_resource_to_api_structure(
#                     children_structure,
#                     resource_settings_value or {},
#                     parent_path=current_path,
#                     first_time_input=first_time_input
#                 )
#                 if nested_dict or is_required:
#                     adapted_resource_settings[field_name] = nested_dict
#
#             elif child_structure:
#                 if resource_settings_value and isinstance(resource_settings_value, dict):
#                     adapted_resource_settings[field_name] = {}
#                     for key, value in resource_settings_value.items():
#                         adapted_resource_settings[field_name][key] = value
#
#         else:
#             if resource_settings_value is not None and not is_empty(resource_settings_value):
#                 adapted_resource_settings[field_name] = resource_settings_value
#             elif is_required:
#                 logger.debug(f"Field '{current_path}', type '{field_type}' is required.")
#                 log_message(LogType.WARNING, "The following fields are required to proceed:")
#                 new_value = get_user_value_input(current_path, field_type)
#                 adapted_resource_settings[field_name] = new_value
#             else:
#                 logger.debug(f"Field '{current_path}' was omitted because it is empty or None.")
#
#     return adapted_resource_settings
#
#
# def adapt_resource_to_api_structure(
#         profile: ProfileUserContext,
#         resource_url: str,
#         resource_settings: dict
# ) -> dict:
#     resource_structure = get_resource_settings_structure(profile, resource_url)
#     if not resource_structure:
#         return resource_settings
#
#     adapted_resource = _adapt_resource_to_api_structure(resource_structure, resource_settings)
#     return adapted_resource
