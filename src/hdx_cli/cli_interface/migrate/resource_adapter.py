import re

from ...library_api.common.exceptions import ActionNotAvailableException, HttpException
from ...library_api.common.logging import get_logger
from ...models import ProfileUserContext
from ..common.undecorated_click_commands import basic_options
from ..migrate.helpers import confirm_action

logger = get_logger()


def prompt_user_for_value(key: str, message=None, default=None):
    if not message:
        default_message = f" (default: {default})" if default else ""
        prompt_message = f"*  Please, provide a value for '{key}'{default_message}"
    else:
        prompt_message = message
    prompt_message = f"{prompt_message}: [!i]"

    logger.info(prompt_message)
    user_input = input().strip()
    return user_input if user_input else default


def _adapt_table_merge_pools(pools: dict) -> dict | None:
    logger.info("In progress")
    logger.info(f"{' Merge Pools Settings ':*^40}")
    logger.info("* Current merge pools settings for SOURCE table")
    for key, value in pools.items():
        logger.info(f"*  {key}: {value}")

    updated_pools = {}
    if not confirm_action("* Remove these settings for the TARGET table?"):
        for key in pools.keys():
            new_value = prompt_user_for_value(key)
            if new_value:
                updated_pools[key] = new_value

    logger.info(f"{'*' * 40:<42} -> [!n]")
    return updated_pools if updated_pools else None


def _adapt_table_autoingest(autoingest: dict) -> dict | None:
    logger.info("In progress")
    logger.info(f"{' Autoingest Settings ':*^40}")
    logger.info("* Current autoingest settings for SOURCE table")
    for key, value in autoingest.items():
        logger.info(f"*  {key}: {value}")

    if confirm_action("* Remove these settings from the TARGET table?"):
        logger.info(f"{'*' * 40:<42} -> [!n]")
        return None

    for key in ["transform", "source_credential_id", "bucket_credential_id"]:
        new_value = prompt_user_for_value(key)
        if new_value:
            autoingest[key] = new_value
        else:
            autoingest.pop(key, None)

    logger.info(f"{'*' * 40:<42} -> [!n]")
    return autoingest if autoingest else None


def _extract_table_name_from_sql(sql):
    pattern = r"(?i)\bFROM\s+([`\"']?\w+[`\"']?\.\w+)"
    match = re.search(pattern, sql)
    if not match:
        return None, None

    table_name = match.group(1).replace("`", "").replace('"', "").replace("'", "")
    project, table = table_name.split(".")
    return project, table


def _update_parent_in_summary_sql(summary_settings: dict) -> dict:
    logger.info("In progress")
    logger.info(f"{' Summary Settings ':*^40}")

    sql = summary_settings.get("sql")
    summary_sql = summary_settings.get("summary_sql")

    if not sql:
        logger.info("* SQL not found in the summary settings.")
        sql = get_user_value_input("sql", "string")
        logger.info("*")

    current_project, current_table = _extract_table_name_from_sql(sql)

    current_summary_parents = (
        "Not Found"
        if not current_project or not current_table
        else f"{current_project}.{current_table}"
    )
    logger.info(f"* The current parents for the SUMMARY TABLE are: {current_summary_parents}")
    attempts = 3
    while attempts > 0:
        logger.info(
            "*  Enter new project and table in 'project.table' format "
            "(leave blank to keep current): [!i]"
        )
        new_project_table = input().strip()

        if not new_project_table and current_project and current_table:
            new_project, new_table = current_project, current_table
            break

        try:
            new_project, new_table = new_project_table.split(".")
            if new_project and new_table:
                break
            logger.info("*  Invalid input. Ensure 'project.table' format is used.")
        except ValueError:
            logger.info("*  Invalid format. Please enter 'project.table' with a single dot.")
            attempts -= 1

    if attempts == 0:
        logger.info("*  Maximum attempts reached. Keeping current project.table.")
        new_project, new_table = current_project, current_table

    summary_settings["sql"] = sql.replace(current_project, new_project).replace(
        current_table, new_table
    )
    if summary_sql:
        summary_settings["summary_sql"] = summary_sql.replace(current_project, new_project).replace(
            current_table, new_table
        )

    logger.info(f"{'*' * 40:<42} -> [!n]")
    return summary_settings


def normalize_project(project: dict, reuse_partitions: bool) -> dict:
    if not reuse_partitions:
        project.pop("uuid", None)

    return project


def normalize_summary_table(summary_settings: dict) -> dict:
    return _update_parent_in_summary_sql(summary_settings)


def normalize_table(table: dict, reuse_partitions: bool) -> dict:
    if not reuse_partitions:
        table.pop("uuid", None)

    table_settings = table.get("settings", {})

    # Summary
    if table.get("type") == "summary":
        summary_settings = table_settings.get("summary", {})
        table_settings["summary"] = normalize_summary_table(summary_settings)

    # Autoingest
    autoingest = table_settings.get("autoingest")
    filtered_autoingest = []
    if autoingest and isinstance(autoingest, list):
        for element in autoingest:
            if isinstance(element, dict) and element.get("enabled"):
                updated_element = _adapt_table_autoingest(element)
                if updated_element:
                    filtered_autoingest.append(updated_element)

    if filtered_autoingest:
        table_settings["autoingest"] = filtered_autoingest
    else:
        table_settings.pop("autoingest", None)

    # Merge pools
    merge = table_settings.get("merge")
    if merge and isinstance(merge, dict) and (pools := merge.get("pools")):
        updated_pools = _adapt_table_merge_pools(pools)
        if updated_pools:
            merge["pools"] = updated_pools
        else:
            merge.pop("pools", None)

    return table


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
        user_input = prompt_user_for_value(field_name, message=f"*  {field_name} ({field_type})")

        if not user_input:
            logger.info("*  Invalid value. Please, try again.")
            continue

        if "integer" in field_type:
            try:
                return int(user_input)
            except (ValueError, TypeError):
                logger.info("*  Invalid integer. Please enter a valid number.")
        elif "list" in field_type:
            return [item.strip() for item in user_input.split(",") if item.strip()]
        elif "boolean" in field_type:
            if user_input.lower() in ("1", "true", "t", "yes", "y"):
                return True
            if user_input.lower() in ("0", "false", "f", "no", "n"):
                return False
            logger.info("*  Invalid value. Please enter 'true' or 'false'.")
        elif "decimal" in field_type:
            try:
                return float(user_input)
            except (ValueError, TypeError):
                logger.info("*  Invalid value. Please enter a valid decimal number.")
        else:
            return user_input


def _adapt_resource_to_api_structure(
    resource_structure: dict, resource_settings: dict | None, parent_path="", first_time_input=True
) -> (dict, bool):
    def is_empty(value):
        return value in (None, {}, [], "")

    # Filter out read-only fields
    filtered_structure = {
        k: v for k, v in resource_structure.items() if not v.get("read_only", False)
    }

    adapted_resource_settings = {}
    for field_name, field_props in filtered_structure.items():
        is_required = field_props.get("required", False)
        field_type = field_props.get("type", "")
        resource_settings_value = resource_settings.get(field_name) if resource_settings else None

        current_path = f"{parent_path}.{field_name}" if parent_path else field_name

        if field_type == "nested object":
            children_structure = field_props.get("children", {})
            child_structure = field_props.get("child", None)

            if children_structure:
                if not resource_settings_value and not is_required:
                    continue
                nested_dict, first_time_input = _adapt_resource_to_api_structure(
                    children_structure,
                    resource_settings_value or {},
                    parent_path=current_path,
                    first_time_input=first_time_input,
                )
                if nested_dict or is_required:
                    adapted_resource_settings[field_name] = nested_dict

            elif child_structure:
                if resource_settings_value and isinstance(resource_settings_value, dict):
                    adapted_resource_settings[field_name] = {}
                    for key, value in resource_settings_value.items():
                        adapted_resource_settings[field_name][key] = value

        else:
            if resource_settings_value is not None and not is_empty(resource_settings_value):
                adapted_resource_settings[field_name] = resource_settings_value
            elif is_required:
                if first_time_input:
                    logger.info("In progress")
                    logger.info(f"{' Required Fields ':*^40}")
                    logger.info("* The following fields are required to proceed:")
                    first_time_input = False

                new_value = get_user_value_input(current_path, field_type)
                adapted_resource_settings[field_name] = new_value
            else:
                logger.debug(f"Field '{current_path}' was omitted because it is empty or None.")

    return adapted_resource_settings, first_time_input


def adapt_resource_to_api_structure(
    profile: ProfileUserContext, resource_path: str, resource_settings: dict
) -> dict:
    try:
        resource_structure = basic_options(profile, resource_path)
    except (HttpException, ActionNotAvailableException) as e:
        logger.debug(f"Error fetching resource options: {e}")
        return resource_settings

    adapted_resource, first_time_input = _adapt_resource_to_api_structure(
        resource_structure, resource_settings
    )

    if not first_time_input:
        logger.info(f"{'*' * 40:<42} -> [!n]")

    return adapted_resource
