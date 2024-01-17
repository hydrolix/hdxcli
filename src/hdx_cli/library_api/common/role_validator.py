from typing import List, Optional, Union
import re
import uuid
from pydantic import BaseModel

from ..userdata.token import AuthInfo
from ..common import rest_operations as rest_ops


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

    permissions_list = get_permissions_list(profile, resource_path)
    AVAILABLE_SCOPE_TYPE = [item['scope_type'] for item in permissions_list]
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
    pattern = r'^[a-zA-Z0-9_-]+$'
    return bool(re.match(pattern, input_string))


def is_valid_uuid(value) -> bool:
    try:
        uuid.UUID(value, version=4)
        return True
    except ValueError:
        return False


def get_permissions_list(profile, resource_path) -> list:
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    list_url = f'{scheme}://{hostname}{resource_path}permissions/'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    permissions_list = rest_ops.list(list_url,
                                     headers=headers,
                                     timeout=timeout)
    return permissions_list


def get_permissions_by_scope_type(profile, resource_path, scope_type=None) -> list:
    permissions_list = get_permissions_list(profile, resource_path)
    response = []

    for item in permissions_list:
        if scope_type and item.get('scope_type') == scope_type:
            return item.get('permissions')

        elif not scope_type:
            response += item.get('permissions')

    # Represent all possible permissions without duplication.
    return list(set(response))


def get_role_data_from_standard_input(profile, resource_path) -> Union[Role, None]:
    input_role_name = None

    # ROLE NAME SECTION
    while not input_role_name:
        input_role_name = input("Enter the name for the new role: ").strip()
        if not input_role_name or not is_valid_rolename(input_role_name):
            print('Invalid role name, please try again')
            input_role_name = None

    policies = []
    add_another_policy = True

    # POLICY
    while add_another_policy:
        policy_details = get_data_for_policy(profile, resource_path)
        policies.append(policy_details)

        add_another_policy = input("Do you want to add another Policy? [Y/n]: ").lower() == 'y'

    role_to_create = Role(name=input_role_name, policies=policies)
    _display_role_details(role_to_create)

    confirm_creation = input("Confirm the creation of the new role? [Y/n]: ").lower()
    if confirm_creation != 'y':
        role_to_create = None

    return role_to_create


def modify_role_data_from_standard_input(profile, resource_path, role: Role) -> Union[Role, None]:
    print(f"Starting role editing for: {role.name}")
    print("-" * 40)
    input_role_name = input("Enter the new name for the role (press enter to skip): ").strip()

    while input_role_name and not is_valid_rolename(input_role_name):
        input_role_name = input('Invalid role name, please try again (press enter to skip): ')

    role.name = input_role_name if input_role_name else role.name

    selected_option = None
    print("What would you want to do?")
    while not selected_option:
        print("1. Add a new policy")
        print("2. Modify an existing policy")
        print("3. Delete a policy")
        selected_option = input("Please select an option: ").strip()

        if selected_option not in function_mapping.keys():
            print("Invalid option, please try again")
            selected_option = None

    func_to_call = (globals()
                    .get(function_mapping
                         .get(selected_option)))
    role_to_update = func_to_call(profile, resource_path, role)

    _display_role_details(role_to_update)

    confirm_creation = input("Confirm the update of the role? [Y/n]: ").lower()
    if confirm_creation != 'y':
        role_to_update = None

    return role_to_update


function_mapping = {
    '1': 'add_policy',
    '2': 'modify_policy',
    '3': 'delete_policy'
}


def get_data_for_policy(profile, resource_path) -> Policy:
    has_scope = input("Adding a Policy, does it have a specific scope? [Y/n]: ").lower() == 'y'

    scope_type = None
    scope_id = None
    # SCOPE SECTION
    if has_scope:
        scope_type = input("Specify the type of scope for the role (e.g., project): ").lower()

        while not is_valid_scope_type(profile, resource_path, scope_type):
            print(f"Invalid scope type. Please choose from the available types: "
                  f"{', '.join(get_available_scope_type_list(profile, resource_path))}.")
            scope_type = input(
                "Specify the type of scope for the role: ").lower()

        scope_id = input("Provide the 'uuid' for the specified scope: ")
        while not is_valid_uuid(scope_id):
            print("Invalid UUID format, please try again")
            scope_id = input("Provide the 'uuid' for the specified scope: ")

    # PERMISSIONS SECTION
    selected_permissions = _select_permissions_from_scope(profile, resource_path, scope_type)

    return Policy(permissions=selected_permissions,
                  scope_type=scope_type,
                  scope_id=scope_id)


def _select_permissions_from_scope(profile, resource_path, scope_type) -> list:
    permission_list = get_permissions_by_scope_type(profile, resource_path, scope_type)
    last_index = None
    for index, item in enumerate(permission_list, start=1):
        print(f"{index} - {item}")
        last_index = index

    if last_index is not None:
        print(f"{last_index + 1} - All of them")

    selected_permissions = []
    while len(selected_permissions) < 1:
        selected_indices = input(
            "Enter the numbers corresponding to the permissions "
            "you'd want to add (comma-separated): ").split(',')
        selected_indices_list = [int(index.strip()) for index in selected_indices
                                 if index.strip().isdigit()]

        # Check if user selected 'all'
        if last_index is not None and last_index + 1 in selected_indices_list:
            selected_permissions = permission_list
        else:
            selected_permissions = [permission_list[index - 1] for index in selected_indices_list if
                                    0 < index <= len(permission_list)]

        if len(selected_permissions) < 1:
            print("Invalid selection, please try again")

    return selected_permissions


def _remove_permissions_from_policy(permission_list) -> list:
    for index, item in enumerate(permission_list, start=1):
        print(f"{index} - {item}")

    selected_permissions = []
    while len(selected_permissions) < 1:
        selected_indices = input(
            "Enter the numbers corresponding to the permissions "
            "you'd want to add (comma-separated): ").split(',')
        selected_indices_list = [int(index.strip()) for index in selected_indices
                                 if index.strip().isdigit()]

        selected_permissions = [permission_list[index - 1] for index in selected_indices_list if
                                0 < index <= len(permission_list)]
        if len(selected_permissions) < 1:
            print("Invalid selection, please try again")

    return selected_permissions


def _display_role_details(role):
    print("-" * 40)
    print("Review Role Details")
    print("-" * 40)
    print("Role Name:", role.name)
    for index, policy in enumerate(role.policies, start=1):
        print(f"Policy {index}:")
        if policy.scope_type:
            print(f"  Scope Type: {policy.scope_type}")
            print(f"  Scope ID: {policy.scope_id}")
        print(f"  Permissions: {', '.join(policy.permissions)}")


def _display_policies(policies):
    for index, policy in enumerate(policies, start=1):
        print(f"{index}. Policy:", end=' -> ')
        if policy.scope_type:
            print(f"Scope Type: {policy.scope_type}", end=' | ')
            print(f"Scope ID: {policy.scope_id}", end=' | ')
        print(f"Permissions: {', '.join(policy.permissions)}")


def _get_selection(list_size: int,
                   input_text="Please select an option:"
                   ) -> int:
    """
        Prompt the user to select an option and validate the input.
        Returns:adjusted index of the selected option in the list
        (subtracting 1 for zero-based indexing).
    """
    selected_option = None
    while not selected_option:
        selected_option = input(f"{input_text}: ").strip()
        try:
            selected_option = int(selected_option)
            if 1 <= selected_option <= list_size:
                return selected_option - 1
            print("Invalid option, please enter a valid number")
        except ValueError:
            print("Invalid input, please enter a valid integer")
        selected_option = None


def delete_policy(profile, resource_path, role):
    _display_policies(role.policies)
    selected_option = _get_selection(len(role.policies), "Choose a policy to delete")
    del role.policies[selected_option]

    return role


def add_policy(profile, resource_path, role):
    policy_details = get_data_for_policy(profile, resource_path)
    role.policies.append(policy_details)

    return role


def modify_policy(profile, resource_path, role):
    _display_policies(role.policies)
    selected_policy = _get_selection(len(role.policies), "Choose a policy to modify")
    policy = role.policies[selected_policy]

    scope_type = input(f"Specify the scope type (currently: {policy.scope_type}, "
                       f"press enter to skip): ")

    while scope_type and not is_valid_scope_type(profile, resource_path, scope_type):
        print(f"Invalid scope type. Please choose from the available types: "
              f"{', '.join(get_available_scope_type_list(profile, resource_path))}.")
        scope_type = input(
            "Specify the scope type (press enter to skip): ").lower()

    if scope_type:
        scope_id = input("Provide the 'uuid' for the specified scope "
                         f"(currently: {policy.scope_id}): ")
        while not is_valid_uuid(scope_id):
            print("Invalid UUID format, please try again")
            scope_id = input("Provide the 'uuid' for the specified scope: ")

        policy.scope_type = scope_type
        policy.scope_id = scope_id

    modify_permissions = input("Would you want to modify permissions? [Y/n]: ").lower() == 'y'

    if modify_permissions:
        print("What would you want to do?")
        print("1. Add permissions")
        print("2. Remove permissions")
        selected_option = input("Please select an option: ")

        if selected_option == "1":
            # ADD PERMISSIONS
            selected_permissions = set(_select_permissions_from_scope(profile, resource_path, scope_type))
            current_policy_permissions = set(policy.permissions)
            current_policy_permissions.update(selected_permissions)
            policy.permissions = list(current_policy_permissions)
        else:
            # REMOVE PERMISSIONS
            permissions_to_remove = _remove_permissions_from_policy(policy.permissions)
            policy.permissions = [item for item in policy.permissions if item not in permissions_to_remove]

    return role


def update_role_request(profile,
                        resource_path,
                        resource_body):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    update_url = f'{scheme}://{hostname}{resource_path}'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}

    rest_ops.update_with_put(update_url,
                             headers=headers,
                             timeout=timeout,
                             body=resource_body,
                             params=None)
    print(f"Role '{resource_body.get('name')}' updated successfully")
