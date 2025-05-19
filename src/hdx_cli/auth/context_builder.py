import functools
from pathlib import Path

from hdx_cli.auth.api import login
from hdx_cli.auth.auth import authenticate_user
from hdx_cli.auth.service_account import prompt_and_configure_service_account
from hdx_cli.auth.session import fail_if_token_expired, load_session_data, save_session_data
from hdx_cli.auth.utils import chain_calls_ignore_exc
from hdx_cli.config.paths import HDX_CONFIG_DIR, PROFILE_CONFIG_FILE
from hdx_cli.config.profile_settings import load_config_parameters, load_static_profile_config
from hdx_cli.library_api.common.exceptions import HdxCliException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import (
    DEFAULT_TIMEOUT,
    BasicProfileConfig,
    ProfileLoadContext,
    ProfileUserContext,
)

logger = get_logger()


def load_user_context(load_context: ProfileLoadContext, **args) -> ProfileUserContext:
    load_set_params = functools.partial(load_config_parameters, load_context=load_context)
    user_context = chain_calls_ignore_exc(
        load_session_data,
        fail_if_token_expired,
        load_set_params,  # Function to load profile (scheme, default project and table names)
        load_ctx=load_context,  # Parameters to first function
        exctype=HdxCliException,
    )

    # If user_context is None, the chain failed (cache miss, expired token).
    if not user_context:
        cli_password = args.get("password")
        cli_username = args.get("username")

        current_processing_context = None
        profile_name_for_messages = load_context.name

        try:
            # Attempt to load from cache. If this succeeds, cache file exists,
            # so chain_calls_ignore_exc failure was due to an expired token.
            current_processing_context = load_session_data(load_context)

            if not cli_password:  # Only show message if not re-authenticating with args
                logger.info(f"Session token expired for profile '{profile_name_for_messages}'.")
        except HdxCliException:
            # Cache file does not exist. Load base profile from config file.
            current_processing_context = load_static_profile_config(load_context)
            if not (cli_username and cli_password):  # Only show if interactive auth is next
                logger.info(
                    f"Please login to profile '{profile_name_for_messages}' "
                    f"({current_processing_context.hostname}) to continue."
                )

        # CLI arg > profile's default username.
        username_for_auth = cli_username or current_processing_context.username

        auth_info = authenticate_user(
            current_processing_context.hostname,
            current_processing_context.scheme,
            username=username_for_auth,
            password=cli_password,
        )

        user_context = current_processing_context
        user_context.auth = auth_info

        cache_dir_path = (
            Path(args.get("profile_config_file")).parent
            if args.get("profile_config_file")
            else HDX_CONFIG_DIR
        )
        save_session_data(user_context, cache_dir_path)

        # --- Service Account Prompt ---
        # Ask about Service Account only if username/password were NOT provided as CLI args.
        if not (cli_username and cli_password):
            prompt_and_configure_service_account(user_context, cache_dir_path)

    scheme_from_arg = args.get("uri_scheme")
    timeout_from_arg = args.get("timeout")
    if scheme_from_arg is not None:
        user_context.profile_config.scheme = str(scheme_from_arg)
    if timeout_from_arg is not None:
        user_context.timeout = int(timeout_from_arg)

    return user_context


def get_profile(
    profile_name: str,
    cluster_hostname: str,
    cluster_username: str,
    cluster_password: str,
    cluster_uri_scheme: str,
    timeout: int,
) -> ProfileUserContext:
    """
    Get the user profile based on the provided parameters.
    If a profile name is provided, it loads the profile from the configuration file.
    If cluster credentials are provided, it creates an ephemeral profile with those credentials.
    If neither is provided, it raises an exception.
    """
    if profile_name:
        load_context = ProfileLoadContext(name=profile_name, config_file=PROFILE_CONFIG_FILE)
        user_profile = load_user_context(load_context)
    elif cluster_hostname and cluster_username and cluster_password and cluster_uri_scheme:
        user_profile = create_ephemeral_profile(
            cluster_hostname,
            cluster_uri_scheme,
            cluster_username,
            cluster_password,
        )
    else:
        raise HdxCliException("No profile name or cluster credentials provided.")

    # Set the timeout if provided
    user_profile.timeout = timeout or DEFAULT_TIMEOUT
    return user_profile


def create_ephemeral_profile(
    cluster_hostname: str,
    cluster_uri_scheme: str,
    cluster_username: str,
    cluster_password: str,
) -> ProfileUserContext:
    """
    Creates an ephemeral profile for the given cluster. This profile is not saved to disk and
    is used only for the current session.
    """
    profile_context = ProfileLoadContext("ephemeral", PROFILE_CONFIG_FILE)
    profile_config = BasicProfileConfig(hostname=cluster_hostname, scheme=cluster_uri_scheme)
    auth_info = login(
        profile_config.hostname, profile_config.scheme, cluster_username, password=cluster_password
    )
    ephemeral_user_context = ProfileUserContext(
        profile_context=profile_context,
        profile_config=profile_config,
        auth=auth_info,
    )

    return ephemeral_user_context
