from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

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


def prompt_and_configure_service_account(
    user_context: ProfileUserContext, cache_dir_path: Path
) -> None:
    """
    Prompts the user about Service Account usage and configures it if chosen.
    """
    logger.info("\n----- Service Account Configuration -----")
    logger.info("You can configure a Service Account for automated access.")

    ## Check if Service Accounts feature is available
    try:
        available_svc_accounts = find_service_accounts(user_context)
    except HttpException:
        logger.info("No Service Accounts feature available.")
        logger.info("Continuing with your user credentials for this profile.")
        logger.info("\n----- End of Service Account Configuration -----\n")
        return

    options = {
        "1": "Use an existing Service Account",
        "2": "Create a new Service Account",
        "3": "Continue using my user credentials",  # Default
    }
    default_choice_key = "3"

    prompt_message = "\nHow would you like to proceed for future authentications?"
    for key, desc in options.items():
        prompt_message += f"\n  {key}. {desc}"
        if key == default_choice_key:
            prompt_message += " (default)"
    prompt_message += "\nChoose an option: "

    while True:
        user_input = input(prompt_message).strip()
        if not user_input:  # User pressed Enter without typing anything
            chosen_key = default_choice_key
            break
        if user_input in options:
            chosen_key = user_input
            break
        else:
            logger.info(
                f"Invalid option '{user_input}'. "
                f"Please enter a number from {', '.join(options.keys())}."
            )

    # Process the choice
    # Use existing SA
    if chosen_key == "1":
        logger.info("\nConfiguring an existing Service Account.")
        name_to_id_map = {
            sa["name"]: sa["uuid"] for sa in available_svc_accounts if "uuid" in sa and "name" in sa
        }
        while True:
            logger.info("Enter Service Account name: [!n]")
            sa_name = input("").strip()
            if sa_name and (sa_id := name_to_id_map.get(sa_name)):
                break
            logger.info("Invalid or inexistent name. Please, try again.")

        logger.info(f"\nService Account '{sa_name}' selected. Generating token...")
        try:
            _set_service_account_token(user_context, sa_id, cache_dir_path)
            logger.info(
                f"Profile '{user_context.profilename}' is now configured"
                f" to use Service Account '{sa_name}'."
            )
        except HdxCliException as e:
            logger.debug(f"Error configuring Service Account '{sa_name}': {e}")
            logger.info("Continuing with your user credentials for this profile.")

    # Create new SA
    elif chosen_key == "2":
        logger.info("\nGuiding you to create a new Service Account...")
        values = service_account_from_standard_input(user_context)
        if not values:
            raise HdxCliException("Service Account creation aborted.")
        new_sa_name, roles = values

        prompt_roles = ", ".join(roles)
        logger.info(f"Creating Service Account '{new_sa_name}' with roles: {prompt_roles}")
        svc_account = create_service_account(user_context, new_sa_name, roles)

        if not (svc_account and (svc_account_id := svc_account.get("uuid"))):
            raise HdxCliException(
                "Service Account creation failed. Please, check your permissions."
            )
        logger.info(f"\nService Account '{new_sa_name}' created successfully. Generating token...")
        try:
            _set_service_account_token(user_context, svc_account_id, cache_dir_path)
            logger.info(
                f"Profile '{user_context.profilename}' is now configured"
                f" to use Service Account '{new_sa_name}'."
            )
        except HdxCliException as e:
            logger.debug(f"Error configuring Service Account '{new_sa_name}': {e}")
            logger.info("Continuing with your user credentials for this profile.")

    # chosen_key == "3" (Default)
    else:
        logger.info("\nContinuing with your user credentials for this profile.")

    logger.info("\n----- End of Service Account Configuration -----\n")


def service_account_from_standard_input(
    user_context: ProfileUserContext,
) -> Optional[tuple[str, list[str]]]:
    try:
        while True:
            logger.info(f"Enter the name for the Service Account: [!n]")
            name_input = input("").strip()
            if name_input:
                break
            logger.info("Invalid name. Please, try again.")

        available_roles = find_roles(user_context)
        if not available_roles:
            raise HdxCliException("No roles found. Please, check your permissions.")
        id_to_name_map = {
            role["id"]: role["name"] for role in available_roles if "id" in role and "name" in role
        }
        logger.info("Available roles ID to assign to the Service Account: ")
        for role_id, role_name in id_to_name_map.items():
            logger.info(f" {role_id}. {role_name}")

        while True:
            # 3. Get user input
            selected_role_names = []
            logger.info(
                "Enter the IDs of the roles you want to assign, separated by commas (e.g., '1,3'): [!n]"
            )
            user_input = input("").strip()

            if not user_input:  # User pressed Enter (empty input)
                logger.info("Invalid input. Please, try again.")
                continue

            # Split the input and validate
            potential_ids = user_input.replace(" ", "").split(",")
            for p_id in potential_ids:
                if role_name := id_to_name_map.get(int(p_id)):
                    selected_role_names.append(role_name)

            if not selected_role_names:
                logger.info("No valid role IDs provided. Please, try again.")
                continue

            return name_input, selected_role_names
    except (KeyboardInterrupt, EOFError) as e:
        logger.debug(f"Configuration process cancelled by user: {e}")
    except HdxCliException as e:
        logger.debug(f"An error occurred during configuration: {e}")

    return None


def _set_service_account_token(
    user_context: ProfileUserContext,
    svc_account_id: str,
    cache_dir_path: Path,
) -> None:
    """
    Generate a service account token for the given service account ID
    and update the user context.
    """
    token_data = create_service_account_token(user_context, svc_account_id)

    token_expiration_time = datetime.now() + timedelta(
        seconds=token_data["expires_in"] - (token_data["expires_in"] * 0.05)
    )
    # Update user_context with the new token
    user_context.auth.token = token_data["access_token"]
    user_context.auth.expires_at = token_expiration_time
    user_context.auth.method = "service_account"

    # Update the profile cache file with the new token/method
    save_session_data(user_context, cache_dir_path)
