import re
import uuid
from typing import List, Optional, Union

from pydantic import BaseModel
from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from rich.console import Console
from rich.rule import Rule
from rich.table import Table

from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()

AVAILABLE_SCOPE_TYPE = []


class Policy(BaseModel):
    permissions: List[str]
    scope_type: Optional[str] = None
    scope_id: Optional[str] = None


class Role(BaseModel):
    id: Optional[int] = None
    name: str
    policies: List[Policy]


def get_available_scope_type_list(profile: ProfileUserContext, resource_path: str) -> list:
    global AVAILABLE_SCOPE_TYPE

    if AVAILABLE_SCOPE_TYPE:
        return AVAILABLE_SCOPE_TYPE

    permissions_list = access_resource(profile, [("permissions", None)], base_path=resource_path)
    AVAILABLE_SCOPE_TYPE = [item["scope_type"] for item in permissions_list]
    return AVAILABLE_SCOPE_TYPE


def is_valid_rolename(input_string: str) -> bool:
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


def get_permissions_by_scope_type(
    profile: ProfileUserContext,
    resource_path: str,
    scope_type: str=None,
) -> list:
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
    profile: ProfileUserContext, resource_path: str, role_name: str
) -> Union[Role, None]:
    """Guides the user through creating a role interactively."""
    try:
        policies = []
        add_another_policy = True
        while add_another_policy:
            policy_details = _get_data_for_policy(profile, resource_path)
            policies.append(policy_details)
            add_another_policy = inquirer.confirm(
                message="Do you want to add another Policy?", default=False
            ).execute()

        role_to_create = Role(name=role_name, policies=policies)
        _display_role_details(role_to_create)

        if inquirer.confirm(message="Confirm the creation of the new role?", default=True).execute():
            return role_to_create
        return None
    except KeyboardInterrupt:
        return None


def modify_role_data_from_standard_input(
    profile: ProfileUserContext,
    resource_path: str,
    role: Role,
) -> Union[Role, None]:
    """Guides the user through editing a role interactively."""
    try:
        new_name = inquirer.text(
            message="Enter the new name for the role (press enter to skip):",
            default=role.name,
            validate=is_valid_rolename,
            invalid_message="Invalid role name format.",
        ).execute()
        role.name = new_name

        while True:
            choice = inquirer.select(
                message="What would you like to do?",
                choices=[
                    Choice(add_policy_to_role, "Add a new policy"),
                    Choice(modify_policy_from_role, "Modify an existing policy"),
                    Choice(remove_policy_from_role, "Remove a policy"),
                    Choice(None, "Finish editing"),
                ],
            ).execute()

            if choice is None:
                break
            role = choice(profile, resource_path, role)

        _display_role_details(role)

        if inquirer.confirm(message="Confirm the update of the role?", default=True).execute():
            return role
        return None
    except KeyboardInterrupt:
        return None


def _get_data_for_policy(profile: ProfileUserContext, resource_path: str) -> Policy:
    """Interactively prompts for the data to create a single policy."""
    has_scope = inquirer.confirm(
        message="Does this policy have a specific scope (e.g., for a single project)?",
        default=False,
    ).execute()

    scope_type, scope_id = None, None
    if has_scope:
        scope_type = inquirer.select(
            message="Select the scope type:",
            choices=get_available_scope_type_list(profile, resource_path),
        ).execute()

        scope_id = inquirer.text(
            message=f"Enter the UUID for the '{scope_type}':",
            validate=is_valid_uuid,
            invalid_message="Invalid UUID format.",
        ).execute()

    permissions = _select_permissions_from_scope(profile, resource_path, scope_type)
    return Policy(permissions=permissions, scope_type=scope_type, scope_id=scope_id)


def _select_permissions_from_scope(profile: ProfileUserContext, resource_path: str, scope_type: str) -> list:
    """Interactively prompts to select permissions for a given scope."""
    available_permissions = get_permissions_by_scope_type(profile, resource_path, scope_type)
    if not available_permissions:
        logger.warning("No permissions available for this scope.")
        return []

    choices = [Choice(value=p, name=p) for p in available_permissions]
    selected = inquirer.checkbox(
        message="Select permissions for this policy:",
        choices=choices,
        validate=lambda result: len(result) >= 1,
        invalid_message="You must select at least one permission.",
    ).execute()
    return selected


def _remove_permissions_from_policy(permission_list: list) -> list:
    """Interactively prompts to select permissions to remove from a list."""
    if not permission_list:
        logger.info("No permissions to remove.")
        return []

    choices = [Choice(value=p, name=p) for p in permission_list]
    selected = inquirer.checkbox(
        message="Select permissions to remove:",
        choices=choices,
    ).execute()
    return selected


def _display_role_details(role: Role):
    """Displays role details in a table format using rich."""
    console.print(Rule("Review Role Details", style="dim", characters="─"))
    console.print(f"[bold]Role Name:[/] {role.name}\n")

    if not role.policies:
        console.print("This role has no policies.\n", style="dim")
        return

    console.print("[bold]Policies:[/]")
    for index, policy in enumerate(role.policies, start=1):
        policy_table = Table(show_header=False, box=None, padding=(0, 1), pad_edge=False)
        policy_table.add_column(style="dim", no_wrap=True)
        policy_table.add_column(overflow="fold")

        if policy.scope_type:
            policy_table.add_row("Scope Type:", policy.scope_type)
            policy_table.add_row("Scope ID:", policy.scope_id)
        else:
            policy_table.add_row("Scope Type:", "Global")

        policy_table.add_row("Permissions:", ", ".join(policy.permissions))
        console.print(policy_table)
        console.print()


def _select_policy_from_list(policies: list[Policy], message: str) -> Optional[int]:
    """Displays a selectable list of policies and returns the index of the chosen one."""
    if not policies:
        logger.info("This role has no policies to select.")
        return None

    choices = [
        Choice(
            value=i,
            name=f"Scope: {p.scope_type or 'Global'} | Permissions: {len(p.permissions)}",
        )
        for i, p in enumerate(policies)
    ]
    choices.append(Choice(value=None, name="Cancel"))

    try:
        return inquirer.select(message=message, choices=choices, default=None).execute()
    except KeyboardInterrupt:
        return None


def remove_policy_from_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    """Interactively removes a policy from a role."""
    if not role.policies:
        logger.info("Role has no policies to remove.")
        return role

    selected_index = _select_policy_from_list(role.policies, "Choose a policy to remove")
    if selected_index is not None:
        del role.policies[selected_index]
        logger.info("Policy removed.")
    return role


def add_policy_to_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    """Interactively adds a new policy to a role."""
    policy_details = _get_data_for_policy(profile, resource_path)
    role.policies.append(policy_details)
    logger.info("Policy added.")
    return role


def modify_policy_from_role(profile: ProfileUserContext, resource_path: str, role: Role) -> Role:
    """Interactively modifies an existing policy on a role."""
    if not role.policies:
        logger.info("Role has no policies to modify.")
        return role

    selected_index = _select_policy_from_list(role.policies, "Choose a policy to modify")
    if selected_index is None:
        return role

    policy_to_edit = role.policies[selected_index]

    while True:
        edit_choice = inquirer.select(
            message="What do you want to modify?",
            choices=[
                Choice("add", "Add Permissions"),
                Choice("remove", "Remove Permissions"),
                Choice(None, "Finish modifying this policy"),
            ],
        ).execute()

        if edit_choice is None:
            break

        if edit_choice == "add":
            new_permissions = _select_permissions_from_scope(
                profile, resource_path, policy_to_edit.scope_type
            )
            updated_permissions = set(policy_to_edit.permissions) | set(new_permissions)
            policy_to_edit.permissions = sorted(list(updated_permissions))
            logger.info("Permissions added.")

        elif edit_choice == "remove":
            permissions_to_remove = _remove_permissions_from_policy(policy_to_edit.permissions)
            policy_to_edit.permissions = [
                p for p in policy_to_edit.permissions if p not in permissions_to_remove
            ]
            logger.info("Permissions removed.")
    return role
