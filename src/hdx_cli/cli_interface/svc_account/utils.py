import json
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any

import jwt

from hdx_cli.auth.session import save_session_data
from hdx_cli.cli_interface.common.cached_operations import find_roles
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create, basic_delete
from hdx_cli.library_api.common.exceptions import LogicException, ResourceNotFoundException
from hdx_cli.models import ProfileUserContext, AuthInfo


def _parse_duration_to_iso(duration: str) -> Optional[str]:
    """Parses a duration string (e.g., '30d') into an ISO 8601 UTC timestamp."""
    if not duration:
        return None

    match = re.match(r"(\d+)([dhmy])", duration.lower())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use 'd' (days), 'h' (hours), "
            f"'m' (minutes), or 'y' (years)."
        )

    value, unit = int(match.group(1)), match.group(2)
    now = datetime.now(timezone.utc)

    if unit == "d":
        delta = timedelta(days=value)
    elif unit == "h":
        delta = timedelta(hours=value)
    elif unit == "m":
        delta = timedelta(minutes=value)
    elif unit == "y":
        delta = timedelta(days=value * 365)
    else:
        # This path should not be reachable due to the regex
        raise ValueError("Invalid duration unit.")

    future_date = now + delta
    # Format to ISO 8601
    return future_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def create_service_account_token(
    profile: ProfileUserContext,
    svc_account_id: str,
    duration: str = None,
) -> Dict:
    """Requests a token for a service account and returns its decoded details."""
    resource_path = f"/config/v1/service_accounts/{svc_account_id}/tokens/"

    body = {}
    if expiry_iso := _parse_duration_to_iso(duration):
        body["expiry"] = expiry_iso

    response_data = basic_create(profile, resource_path, body=body).json()
    try:
        token_str = response_data["token"]
        decoded_payload = jwt.decode(token_str, options={"verify_signature": False})
        iat = decoded_payload.get("iat", 0)
        exp = decoded_payload.get("exp", 0)
        expires_in = exp - iat if exp and iat else 0

        return {
            "access_token": token_str,
            "expires_in": int(expires_in),
            "token_type": "Bearer",
        }

    except (TypeError, KeyError, jwt.PyJWTError) as e:
        raise LogicException(
            f"Invalid API response or token format: {e}. Raw token response: {json.dumps(response_data)}"
        ) from e


def set_token_as_auth(profile: ProfileUserContext, token_dict: Dict[str, Any]):
    """Updates the current profile to use the provided token for authentication."""
    token_expiration_time = datetime.now() + timedelta(seconds=token_dict.get("expires_in") * 0.95)
    auth_info = AuthInfo(
        token=token_dict.get("access_token"),
        expires_at=token_expiration_time,
        token_type=token_dict.get("token_type"),
        org_id=profile.auth.org_id,
        username=profile.auth.username,
        method="service_account",
    )

    profile.auth = auth_info
    save_session_data(profile)


def revoke_all_service_account_tokens(profile: ProfileUserContext, svc_account_id: str):
    """Revokes all existing tokens for a given service account."""
    scheme = profile.scheme
    hostname = profile.hostname
    target_url = f"{scheme}://{hostname}/config/v1/service_accounts/{svc_account_id}/tokens/"

    basic_delete(profile, resource_path="", resource_name="", url=target_url)


def create_service_account(
    profile: ProfileUserContext,
    svc_account_name: str,
    roles: list,
    svc_account_path: str = "/config/v1/service_accounts/",
) -> dict:
    """Creates a new service account and assigns roles to it."""
    # Validate roles exist before creating the SA to fail early.
    validate_roles_exist(profile, roles)

    svc_account = basic_create(profile, svc_account_path, svc_account_name).json()
    svc_account_id = svc_account.get("uuid")
    if not svc_account_id:
        raise LogicException("Service account UUID not found in response.")

    update_roles(profile, svc_account_id, roles)
    return svc_account


def validate_roles_exist(profile: ProfileUserContext, roles: list) -> None:
    """Validates that all requested role names exist in the target cluster."""
    existing_roles = find_roles(profile)
    if not existing_roles:
        raise ResourceNotFoundException("No roles found.")

    indexed_existing_roles = {role.get("name", "") for role in existing_roles}
    for role_name in roles:
        if role_name not in indexed_existing_roles:
            raise ResourceNotFoundException(f"Role with name '{role_name}' not found.")


def update_roles(
    profile: ProfileUserContext,
    svc_account_id: str,
    roles: list,
    action: str = "add",
) -> None:
    """Adds or removes a list of roles from a service account."""
    if action not in ("add", "remove"):
        raise ValueError("Action must be either 'add' or 'remove'.")

    body = {"roles": list(roles)}
    path = f"/config/v1/users/{svc_account_id}/{action}_roles/"
    basic_create(profile, path, body=body)
