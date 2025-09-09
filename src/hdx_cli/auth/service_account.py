import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from InquirerPy import inquirer
from InquirerPy.base.control import Choice
from InquirerPy.exceptions import InvalidArgument

from hdx_cli.auth.session import save_session_data
from hdx_cli.cli_interface.common.cached_operations import find_roles, find_service_accounts
from hdx_cli.cli_interface.svc_account.utils import (
    create_service_account,
    create_service_account_token,
)
from hdx_cli.library_api.common.exceptions import HdxCliException, HttpException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import ProfileUserContext

logger = get_logger()


def _is_valid_duration_format(duration: str) -> bool:
    """Checks if the duration string is valid or empty."""
    # Allow empty string for default duration
    if not duration:
        return True
    return bool(re.match(r"^\d+[dhmy]$", duration.lower()))


def _prompt_for_token_duration() -> Optional[str]:
    """Prompts the user for an optional token duration."""
    try:
        duration = inquirer.text(
            message="Enter token duration (e.g., 30d, 12h, 1y) or leave blank for default (1 year):",
            validate=_is_valid_duration_format,
            invalid_message="Invalid format. Use 'd' (days), 'h' (hours), 'm' (minutes), or 'y' (years).",
        ).execute()
        return duration
    except (KeyboardInterrupt, InvalidArgument):
        return None


def _prompt_for_existing_sa(user_context: ProfileUserContext) -> Optional[Tuple[str, str]]:
    """Shows a selectable list of existing SAs and returns the chosen (UUID, duration)."""
    try:
        accounts = find_service_accounts(user_context)
        if not accounts:
            logger.info("No existing Service Accounts found.")
            return None

        choices = [Choice(value=acc["uuid"], name=acc["name"]) for acc in accounts]
        sa_id = inquirer.select(
            message="Select an existing Service Account:",
            choices=choices,
            default=None,
        ).execute()

        if not sa_id:
            return None

        duration = _prompt_for_token_duration()
        return sa_id, duration
    except KeyboardInterrupt:
        return None


def _prompt_for_new_sa(user_context: ProfileUserContext) -> Optional[Tuple[str, List[str], str]]:
    """Guides the user through creating a new SA and returns its (name, roles, duration)."""
    try:
        sa_name = inquirer.text(
            message="What is the name for the new Service Account?",
            validate=lambda result: len(result) > 0,
            invalid_message="Name cannot be empty.",
        ).execute()

        available_roles = find_roles(user_context)
        if not available_roles:
            logger.warning(
                "No roles available to assign. The Service Account will be created without roles."
            )
            roles = []
        else:
            role_choices = [
                Choice(value=role["name"], name=role["name"]) for role in available_roles
            ]
            roles = inquirer.checkbox(
                message="Select roles to assign (space to select, enter to confirm):",
                choices=role_choices,
                validate=lambda result: len(result) >= 1,
                invalid_message="You must select at least one role.",
            ).execute()

        duration = _prompt_for_token_duration()
        return sa_name, roles, duration
    except (KeyboardInterrupt, InvalidArgument):
        return None


def prompt_and_configure_service_account(user_context: ProfileUserContext) -> None:
    """Prompts the user about Service Account usage and configures it if chosen."""
    logger.info("\n----- Service Account Configuration -----")
    logger.info("A Service Account can be configured for automated access.")

    try:
        # Check if the feature is available by trying to fetch accounts.
        find_service_accounts(user_context)
    except HttpException:
        logger.info("Service Accounts feature not available on this cluster.")
        logger.info("Continuing with user credentials for this profile.")
        logger.info("----- End of Service Account Configuration -----\n")
        return

    try:
        choice = inquirer.select(
            message="How would you like to authenticate for this profile?",
            choices=[
                Choice(value="existing", name="Use an existing Service Account"),
                Choice(value="create", name="Create a new Service Account"),
                Choice(value="user", name="Continue with my user credentials for this session"),
            ],
            default="user",
        ).execute()

        # Process the choice
        if choice == "existing":
            if values := _prompt_for_existing_sa(user_context):
                sa_id, duration = values
                sa_name = next(
                    (
                        acc["name"]
                        for acc in find_service_accounts(user_context)
                        if acc["uuid"] == sa_id
                    ),
                    "Unknown",
                )
                logger.info(f"Configuring Service Account '{sa_name}'...")
                _set_service_account_token(user_context, sa_id, duration)
                logger.info(
                    f"Profile '{user_context.profilename}' is now configured to use Service Account '{sa_name}'."
                )
            else:
                logger.info("No Service Account selected. Continuing with user credentials.")

        elif choice == "create":
            if values := _prompt_for_new_sa(user_context):
                new_sa_name, roles, duration = values
                prompt_roles = ", ".join(roles)
                logger.info(
                    f"Creating Service Account '{new_sa_name}' with roles: {prompt_roles}..."
                )
                svc_account = create_service_account(user_context, new_sa_name, roles)

                if not (svc_account and (svc_account_id := svc_account.get("uuid"))):
                    raise HdxCliException(
                        "Service Account creation failed. Please check permissions."
                    )

                logger.info(f"Service Account '{new_sa_name}' created. Generating token...")
                _set_service_account_token(user_context, svc_account_id, duration)
                logger.info(
                    f"Profile '{user_context.profilename}' is now configured to use Service Account '{new_sa_name}'."
                )
            else:
                logger.info("Service Account creation cancelled. Continuing with user credentials.")

        else:  # choice == "user"
            logger.info("Continuing with user credentials for this profile.")

    except (KeyboardInterrupt, InvalidArgument):
        logger.info("\nConfiguration cancelled. Continuing with user credentials.")
    except HdxCliException as e:
        logger.debug(f"An error occurred during Service Account configuration: {e}")
        logger.info("Configuration failed. Continuing with user credentials.")

    logger.info("----- End of Service Account Configuration -----\n")


def _set_service_account_token(
    user_context: ProfileUserContext,
    svc_account_id: str,
    duration: Optional[str] = None,
) -> None:
    """
    Generates a service account token and updates the user context and session.
    """
    token_data = create_service_account_token(user_context, svc_account_id, duration=duration)

    # Calculate a safe expiration time, leaving a 5% buffer
    token_expiration_time = datetime.now() + timedelta(seconds=token_data["expires_in"] * 0.95)
    # Update user_context with the new token
    user_context.auth.token = token_data["access_token"]
    user_context.auth.expires_at = token_expiration_time
    user_context.auth.method = "service_account"

    # Update the profile cache file with the new token/method
    save_session_data(user_context)
