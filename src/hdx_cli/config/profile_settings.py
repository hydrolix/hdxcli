import os
from typing import Optional, Union, overload

import toml
from InquirerPy import inquirer

from hdx_cli.auth.session import delete_session_file
from hdx_cli.config.paths import PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.exceptions import (
    HdxCliException,
    LogicException,
    ProfileNotFoundException,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import BasicProfileConfig, ProfileLoadContext, ProfileUserContext

logger = get_logger()


def profile_config_from_standard_input(
    hostname: Optional[str] = None, scheme: Optional[str] = None
) -> Optional[BasicProfileConfig]:
    try:
        input_hostname = inquirer.text(
            message="Enter the host address for the profile:",
            default=hostname or "",
            validate=is_valid_hostname,
            invalid_message="Invalid host name format. Please, try again.",
        ).execute()

        use_tls = inquirer.confirm(
            message="Use TLS (https) for connection?",
            default=scheme == "https" if scheme else True,
        ).execute()

        input_scheme = "https" if use_tls else "http"
        return BasicProfileConfig(hostname=input_hostname, scheme=input_scheme)
    except KeyboardInterrupt:
        logger.debug("Configuration process cancelled by user.")
    except HdxCliException as e:
        logger.debug(f"An error occurred during configuration: {e}")
    return None


@overload
def load_static_profile_config(load_profile_context: ProfileLoadContext) -> ProfileUserContext: ...


@overload
def load_static_profile_config(load_profile_name: str) -> ProfileUserContext: ...


def load_static_profile_config(
    load_profile_context: Union[ProfileLoadContext, str],
) -> ProfileUserContext:
    """Loads a profile from a path in disk or from a load context in memory"""
    profile_config_file, profile_name = None, None
    try:
        if isinstance(load_profile_context, ProfileLoadContext):
            profile_config_file = load_profile_context.config_file
            profile_name = load_profile_context.name
        elif isinstance(load_profile_context, str):
            profile_config_file = PROFILE_CONFIG_FILE
            profile_name = load_profile_context
        else:
            raise LogicException("Wrong profile type.")

        with open(profile_config_file, "r", encoding="utf-8") as stream:
            profile_dict = toml.load(stream)[profile_name]
            profile_dict["profile_name"] = profile_name
            profile_dict["profile_config_file"] = profile_config_file

            # Workaround for profiles created with the old version of the CLI.
            # username belongs to the auth info
            try:
                del profile_dict["username"]
            except KeyError:
                pass

            return ProfileUserContext.from_flat_dict(profile_dict)
    except FileNotFoundError as ex:
        raise ProfileNotFoundException(f"File name '{profile_config_file}' not found.") from ex
    except KeyError as key_err:
        raise ProfileNotFoundException(f"Profile name '{profile_name}' not found.") from key_err


def load_config_parameters(
    user_context: ProfileUserContext, load_context: ProfileLoadContext
) -> ProfileUserContext:
    """
    Given a profile to load (load_context) and a profile (user_context),
    it returns the user_context with the config parameters projectname
    and tablename and scheme loaded.
    """
    profile = load_static_profile_config(load_context)
    user_context.profile_config.scheme = profile.scheme
    user_context.projectname = profile.projectname
    user_context.tablename = profile.tablename
    return user_context


def save_profile_config(
    user_context: ProfileUserContext,
    *,
    initial_profile: dict = None,
    logout: bool = True,
) -> None:
    """
    Save or update the profile entry in the profile config file.

    - If `initial_profile` is provided, it will be updated in-place with the new profile.
    - Otherwise, a new profile config dictionary is created.
    - The config is written to `profile_config_file` in TOML format.
    - Any token file associated with this profile is removed afterward.
    """
    if initial_profile is None:
        initial_profile = {}

    # Update the config dictionary with the new profile
    config_data = {user_context.profilename: user_context.as_dict_for_config()}
    initial_profile.update(config_data)

    try:
        os.makedirs(user_context.profile_config_file.parent, exist_ok=True)
    except OSError as e:
        raise HdxCliException(f"Failed to create config directory: {e}")

    # Write updated config to the file
    try:
        with open(user_context.profile_config_file, "w", encoding="utf-8") as config_file:
            toml.dump(initial_profile, config_file)
    except OSError as e:
        raise HdxCliException(f"Failed to write profile config file: {e}")

    # Delete the profile cache if it exists, only if logout is True (default)
    if logout:
        delete_session_file(user_context.profile_context)


def delete_profile_config(profile_context: ProfileLoadContext, initial_profiles: dict) -> None:
    try:
        del initial_profiles[profile_context.name]
    except KeyError as exc:
        raise HdxCliException(
            f"There was an error trying to delete '{profile_context.name}' profile."
        ) from exc

    with open(profile_context.config_file, "w+", encoding="utf-8") as config_file:
        toml.dump(initial_profiles, config_file)

    delete_session_file(profile_context)


def is_valid_scheme(scheme: str) -> bool:
    return scheme in ("https", "http")


def is_valid_hostname(hostname: str) -> bool:
    import re  # pylint:disable=import-outside-toplevel

    if not hostname or len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1]  # strip exactly one dot from the right, if present
    pattern = r"^([\w\d][\w\d\.\-]+[\w\d])(\:([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5]))?$"
    allowed = re.compile(pattern, re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))
