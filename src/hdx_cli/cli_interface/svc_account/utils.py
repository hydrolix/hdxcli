from hdx_cli.cli_interface.common.cached_operations import find_roles
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.exceptions import LogicException, ResourceNotFoundException
from hdx_cli.models import ProfileUserContext


def create_service_account(
    profile: ProfileUserContext,
    svc_account_name: str,
    roles: list,
    svc_account_path: str = "/config/v1/service_accounts/",
) -> dict:
    """
    Create a new service account and assign roles to it.

    :param profile: ProfileUserContext object
    :param svc_account_name: Name of the service account to create
    :param roles: List of roles to assign to the service account
    :param svc_account_path: Path to the service account resource

    :return: None

    :raises ResourceNotFoundException: If any of the requested roles do not exist
    :raises LogicException: If the service account creation fails or lacks a UUID
    """
    # Check that all requested roles exist before creating the service account
    # This avoids creating the service account if any of the roles are invalid
    validate_roles_exist(profile, roles)

    # Create the service account using the provided name and audit flag
    svc_account = basic_create(profile, svc_account_path, svc_account_name).json()
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    # Assign the service account to each valid role using the /add_user/ endpoint
    update_roles(profile, svc_account_id, roles)
    return svc_account


def validate_roles_exist(profile: ProfileUserContext, roles: list) -> None:
    """
    Validate that all requested roles exist.

    :param profile: ProfileUserContext object
    :param roles: List of roles to check

    :return: None

    :raises ResourceNotFoundException: If any of the requested roles do not exist
    """
    existing_roles = find_roles(profile)
    if not existing_roles:
        raise ResourceNotFoundException("No roles found.")

    indexed_existing_roles = {role.get("name", ""): role.get("id") for role in existing_roles}
    for role_name in roles:
        if not indexed_existing_roles.get(role_name):
            raise ResourceNotFoundException(f"Role with name '{role_name}' not found.")


def update_roles(
    profile: ProfileUserContext,
    svc_account_id: str,
    roles: list,
    action: str = "add",
) -> None:
    """
    Add or remove roles from a service account.

    :param profile: ProfileUserContext object
    :param svc_account_id: ID of the service account
    :param roles: List of roles to assign or remove
    :param action: Action to perform: "add" or "remove"

    :return: None

    :raises ValueError: If the action is not "add" or "remove"
    """
    if action not in ("add", "remove"):
        raise ValueError("Action must be either 'add' or 'remove'")

    body = {"roles": list(roles)}
    path = f"/config/v1/users/{svc_account_id}/{action}_roles/"
    basic_create(profile, path, body=body)


def create_service_account_token(profile: ProfileUserContext, svc_account_id: str) -> dict:
    path = f"/config/v1/service_accounts/{svc_account_id}/tokens/"
    response = basic_create(profile, path).json()
    return response.get("token", {})
