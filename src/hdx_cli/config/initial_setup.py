from pathlib import Path

from hdx_cli.config.paths import PROFILE_CONFIG_FILE
from hdx_cli.config.profile_settings import profile_config_from_standard_input, save_profile_config
from hdx_cli.library_api.common.exceptions import HdxCliException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import ProfileLoadContext, ProfileUserContext

logger = get_logger()

DEFAULT_PROFILE_NAME = "default"


def first_time_use_config(profile_config_file=PROFILE_CONFIG_FILE) -> ProfileLoadContext:
    """
    This function is called when the user is running the CLI for the first time.
    """
    logger.info("No configuration found for your Hydrolix cluster.")
    logger.info("Let's create the 'default' profile to get you started.")
    logger.info(f"\n----- Configuring Profile [{DEFAULT_PROFILE_NAME}] -----\n")

    # Basic profile configuration
    profile_config = profile_config_from_standard_input()
    if not profile_config:
        raise HdxCliException("Configuration creation aborted.")

    profile_context = ProfileLoadContext(name=DEFAULT_PROFILE_NAME, config_file=profile_config_file)

    user_context = ProfileUserContext(
        profile_context=profile_context, profile_config=profile_config
    )
    save_profile_config(user_context)
    logger.info(
        f"\nThe configuration for '{user_context.profilename}' profile "
        f"has been created at {user_context.profile_config_file}"
    )
    logger.info(f"{'':-^30}")
    return profile_context


def is_first_time_use(target_config_file=PROFILE_CONFIG_FILE) -> bool:
    return not Path(target_config_file).exists()
