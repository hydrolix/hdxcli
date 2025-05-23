import re
import uuid
from typing import List, Optional, Union

from pydantic import BaseModel

from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import ProfileUserContext

logger = get_logger()

AVAILABLE_SCOPE_TYPE = []


class Policy(BaseModel):
    permissions: List[str]
    scope_type: Optional[str] = None
    scope_id: Optional[str] = None


class Role(BaseModel):
    id: Optional[int] = None
    name: str
    policies: List[Policy]


def get_available_scope_type_list(profile, resource_path) -> list:
    global AVAILABLE_SCOPE_TYPE

    if AVAILABLE_SCOPE_TYPE:
        return AVAILABLE_SCOPE_TYPE

    permissions_list = access_resource(profile, [("permissions", None)], base_path=resource_path)
    AVAILABLE_SCOPE_TYPE = [item["scope_type"] for item in permissions_list]
    return AVAILABLE_SCOPE_TYPE


def is_valid_scope_type(profile, resource_path, scope_type) -> bool:
    """
    Check if the given scope_type is valid.
    """
    return scope_type in get_available_scope_type_list(profile, resource_path)


def is_valid_rolename(input_string) -> bool:
    """
    Validate if the string only contains letters, numbers, underscores, or hyphens.
    """
    pattern = r"^[a-zA-Z0-9_-]+$"
    return bool(re.match(pattern, input_string))


def is_valid_uuid(value) -> bool:
    try:
        uuid.UUID(value, version=4)
        return True
    except ValueError:
        return False


def get_permissions_by_scope_type(profile, resource_path, scope_type=None) -> list:
    permissions_list = access_resource(profile, [("permissions", None)], base_path=resource_path)
    response = []

    for item in permissions_list:
        if scope_type and item.get("scope_type") == scope_type:
            return item.get("permissions")

        if not scope_type:
            response += item.get("permissions")

    # Represent all possible permissions without duplication.
    return list(set(response))


def get_role_data_from_standard_input(
    profile, resource_path: str, role_name: str
) -> Union[Role, None]:
    policies = []
    add_another_policy = True

    # POLICY
    while add_another_policy:
        policy_details = get_data_for_policy(profile, resource_path)
        policies.append(policy_details)

        logger.info("Do you want to add another Policy? [Y/n]: [!n]")
        add_another_policy = input("").lower() == "y"

    role_to_create = Role(name=role_name, policies=policies)
    _display_role_details(role_to_create)

    logger.info("Confirm the creation of the new role? [Y/n]: [!n]")
    confirm_creation = input("").lower()
    if confirm_creation != "y":
        role_to_create = None

    return role_to_create


def modify_role_data_from_standard_input(profile, resource_path, role: Role) -> Union[Role, None]:
    logger.info(f"Starting role editing for: {role.name}")
    logger.info("-" * 40)
    logger.info("Enter the new name for the role (press enter to skip): [!n]")
    input_role_name = input("").strip()

    while input_role_name and not is_valid_rolename(input_role_name):
        logger.info("Invalid role name, please try again (press enter to skip): [!n]")
        input_role_name = input("")

    role.name = input_role_name if input_role_name else role.name

    selected_option = None
    logger.info("What would you want to do?")
    while not selected_option:
        logger.info("1. Add a new policy")
        logger.info("2. Modify an existing policy")
        logger.info("3. Remove a policy")

        logger.info("Please select an option: [!n]")
        selected_option = input("").strip()

        if selected_option not in function_mapping:
            logger.info("Invalid option, please try again")
            selected_option = None

    func_to_call = globals().get(function_mapping.get(selected_option))
    role_to_update = func_to_call(profile, resource_path, role)

    _display_role_details(role_to_update)

    logger.info("Confirm the update of the role? [Y/n]: [!n]")
    confirm_creation = input("").lower()
    if confirm_creation != "y":
        role_to_update = None

    return role_to_update


function_mapping = {
    "1": "add_policy_from_role",
    "2": "modify_policy_from_role",
    "3": "remove_policy_from_role",
}


def get_data_for_policy(profile, resource_path) -> Policy:
    logger.info("Adding a Policy, does it have a specific scope? [Y/n]: [!n]")
    has_scope = input("").lower() == "y"

    scope_type = None
    scope_id = None
    # SCOPE SECTION
    if has_scope:
        logger.info("Specify the scope type for the role (e.g., project): [!n]")
        scope_type = input("").lower()

        while not is_valid_scope_type(profile, resource_path, scope_type):
            logger.info(
                f"Invalid scope type. Please choose from the available types: "
                f"{', '.join(get_available_scope_type_list(profile, resource_path))}."
            )

            logger.info("Specify the scope type for the role: [!n]")
            scope_type = input("").lower()

        logger.info("Provide the 'uuid' for the specified scope: [!n]")
        scope_id = input("")
        while not is_valid_uuid(scope_id):
            logger.info("Invalid UUID format, please try again")
            logger.info("Provide the 'uuid' for the specified scope: [!n]")
            scope_id = input("")

    # PERMISSIONS SECTION
    selected_permissions = _select_permissions_from_scope(profile, resource_path, scope_type)

    return Policy(permissions=selected_permissions, scope_type=scope_type, scope_id=scope_id)


def _select_permissions_from_scope(profile, resource_path, scope_type) -> list:
    permission_list = get_permissions_by_scope_type(profile, resource_path, scope_type)
    last_index = None
    for index, item in enumerate(permission_list, start=1):
        logger.info(f"{index} - {item}")
        last_index = index

    if last_index is not None:
        logger.info(f"{last_index + 1} - All of them")

    selected_permissions = []
    while len(selected_permissions) < 1:
        logger.info(
            "Enter the numbers corresponding to the permissions "
            "you'd want to add (comma-separated): [!n]"
        )
        selected_indices = input("").split(",")
        selected_indices_list = [
            int(index.strip()) for index in selected_indices if index.strip().isdigit()
        ]

        # Check if user selected 'all'
        if last_index is not None and last_index + 1 in selected_indices_list:
            selected_permissions = permission_list
        else:
            selected_permissions = [
                permission_list[index - 1]
                for index in selected_indices_list
                if 0 < index <= len(permission_list)
            ]

        if len(selected_permissions) < 1:
            logger.info("Invalid selection, please try again")

    return selected_permissions


def _remove_permissions_from_policy(permission_list) -> list:
    for index, item in enumerate(permission_list, start=1):
        logger.info(f"{index} - {item}")

    selected_permissions = []
    while len(selected_permissions) < 1:
        logger.info(
            "Enter the numbers corresponding to the permissions "
            "you'd want to add (comma-separated): [!n]"
        )
        selected_indices = input("").split(",")
        selected_indices_list = [
            int(index.strip()) for index in selected_indices if index.strip().isdigit()
        ]

        selected_permissions = [
            permission_list[index - 1]
            for index in selected_indices_list
            if 0 < index <= len(permission_list)
        ]
        if len(selected_permissions) < 1:
            logger.info("Invalid selection, please try again")

    return selected_permissions


def _display_role_details(role):
    logger.info("-" * 40)
    logger.info("Review Role Details")
    logger.info("-" * 40)
    logger.info(f"Role Name: {role.name}")
    for index, policy in enumerate(role.policies, start=1):
        logger.info(f"Policy {index}:")
        if policy.scope_type:
            logger.info(f"  Scope Type: {policy.scope_type}")
            logger.info(f"  Scope ID: {policy.scope_id}")
        logger.info(f"  Permissions: {', '.join(policy.permissions)}")


def _display_policies(policies):
    for index, policy in enumerate(policies, start=1):
        logger.info(f"{index}. Policy: -> [!n]")
        if policy.scope_type:
            logger.info(f"Scope Type: {policy.scope_type} | [!n]")
            logger.info(f"Scope ID: {policy.scope_id} | [!n]")
        logger.info(f"Permissions: {', '.join(policy.permissions)}")


def _get_selection(list_size: int, input_text="Please select an option:") -> int | None:
    """
    Prompt the user to select an option and validate the input.
    Returns:adjusted index of the selected option in the list
    (subtracting 1 for zero-based indexing).
    """
    selected_option = None
    while not selected_option:
        logger.info(f"{input_text}: [!n]")
        selected_option = input("").strip()

        try:
            selected_option = int(selected_option)
            if 1 <= selected_option <= list_size:
                return selected_option - 1
            logger.info("Invalid option, please try again")
        except ValueError:
            logger.info("Invalid input, please enter a valid integer")
        selected_option = None


def remove_policy_from_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    _display_policies(role.policies)
    selected_option = _get_selection(len(role.policies), "Choose a policy to remove")
    del role.policies[selected_option]
    return role


def add_policy_from_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    policy_details = get_data_for_policy(profile, resource_path)
    role.policies.append(policy_details)
    return role


def modify_policy_from_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    _display_policies(role.policies)
    selected_policy = _get_selection(len(role.policies), "Choose a policy to modify")
    policy = role.policies[selected_policy]

    logger.info(
        f"Specify the scope type (currently: {policy.scope_type}, " f"press enter to skip): [!n]"
    )
    scope_type = input("")

    while scope_type and not is_valid_scope_type(profile, resource_path, scope_type):
        logger.info(
            f"Invalid scope type. Please choose from the available types: "
            f"{', '.join(get_available_scope_type_list(profile, resource_path))}."
        )

        logger.info("Specify the scope type (press enter to skip): [!n]")
        scope_type = input("").lower()

    if scope_type:
        logger.info(
            "Provide the 'uuid' for the specified scope " f"(currently: {policy.scope_id}): [!n]"
        )
        scope_id = input("")
        while not is_valid_uuid(scope_id):
            logger.info("Invalid UUID format, please try again")
            logger.info("Provide the 'uuid' for the specified scope: [!n]")
            scope_id = input("")

        policy.scope_type = scope_type
        policy.scope_id = scope_id

    logger.info("Would you want to modify permissions? [Y/n]: [!n]")
    modify_permissions = input("").lower() == "y"

    if modify_permissions:
        logger.info("What would you want to do?")
        logger.info("1. Add permissions")
        logger.info("2. Remove permissions")

        logger.info("Please select an option: [!n]")
        selected_option = input("")

        if selected_option == "1":
            # ADD PERMISSIONS
            selected_permissions = set(
                _select_permissions_from_scope(profile, resource_path, scope_type)
            )
            current_policy_permissions = set(policy.permissions)
            current_policy_permissions.update(selected_permissions)
            policy.permissions = list(current_policy_permissions)

        elif selected_option == "2":
            # REMOVE PERMISSIONS
            permissions_to_remove = _remove_permissions_from_policy(policy.permissions)
            policy.permissions = [
                item for item in policy.permissions if item not in permissions_to_remove
            ]

        else:
            logger.info("Invalid option, please try again")
    return role
