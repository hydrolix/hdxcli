import os
from datetime import datetime
from pathlib import Path

import toml

from hdx_cli.config.paths import HDX_CONFIG_DIR
from hdx_cli.library_api.common.exceptions import (
    CacheFileNotFoundException,
    HdxCliException,
    LogicException,
    TokenExpiredException,
)
from hdx_cli.models import ProfileLoadContext, ProfileUserContext


class CacheDict:
    """A simple cache dictionary"""

    def __init__(self, dict_data: dict, *, _initialized_from_factory=False):
        if not _initialized_from_factory:
            raise LogicException("Must construct CacheDict from factory.")
        self._cache_dict = dict_data

    @classmethod
    def build_from_dict(cls, the_dict: dict):
        """Use a dictionary to build a new cache"""
        return cls(the_dict, _initialized_from_factory=True)

    @classmethod
    def build_from_toml_stream(cls, toml_data_stream: str):
        """Build a cache from a toml data stream"""
        the_dict = toml.load(toml_data_stream)
        return cls.build_from_dict(the_dict)

    def save_to_stream(self, stream):
        """Save cache"""
        toml.dump(self._cache_dict, stream)

    def __getitem__(self, item):
        """Get item"""
        return self._cache_dict[item]

    def get(self, key, default=None):
        """Access internal dictionary"""
        return self._cache_dict.get(key, default)

    def has_key(self, key):
        """Check membership of key"""
        return key in self._cache_dict


def save_session_data(user_context: ProfileUserContext, cache_dir_path: Path) -> None:
    """
    Save a cache file for this profile.
    The profile cache file is saved in cache_dir_path
    """
    if not isinstance(cache_dir_path, Path):
        cache_dir_path = Path(cache_dir_path)
    os.makedirs(cache_dir_path, mode=0o700, exist_ok=True)

    expiration_time_str = user_context.token_expiration.isoformat()
    token_data = {
        "auth_token": user_context.token,
        "token_type": user_context.token_type,
        "expires_at": expiration_time_str,
    }

    cache_content = {
        "org_id": f"{user_context.org_id}",
        "token": token_data,
        "username": f"{user_context.username}",
        "hostname": f"{user_context.hostname}",
        "scheme": f"{user_context.scheme}",
        "method": f"{user_context.method}",
    }

    cache_dict_obj = CacheDict.build_from_dict(cache_content)
    profile_file_path = cache_dir_path / f"{user_context.profilename}"

    with open(profile_file_path, "w", encoding="utf-8") as f:
        cache_dict_obj.save_to_stream(f)


def delete_session_file(profile_context: ProfileLoadContext) -> bool:
    """
    Deletes the session cache file for a specific profile, if it exists.
    This file usually stores the session token and related data like expiry.

    Raises:
        HdxCliException: If an OS-level error occurs during file deletion
                         (e.g., permission denied).
    """
    profile_cache_dir = profile_context.config_file.parent
    profile_cache_path = profile_cache_dir / profile_context.name

    # Check if the specific file exists before trying to delete it
    if not profile_cache_path.is_file():
        return False

    # Attempt to delete the file
    try:
        profile_cache_path.unlink()
        return True
    except OSError as e:
        error_msg = (
            f"Error deleting cache file for profile '{profile_context.name}' "
            f"at path '{profile_cache_path}'."
        )
        raise HdxCliException(error_msg) from e


def load_session_data(load_ctx: ProfileLoadContext) -> ProfileUserContext:
    """
    Load the data from the cache to avoid making a request if possible.
    It searches the token and other additional info to operate such as org_id,
    and other info that might be needed.
    """
    cache = _try_load_profile_cache_data(load_ctx)
    token_info = cache.get("token", {})
    token = token_info.get("auth_token")
    token_type = token_info.get("token_type")
    expires_at = token_info.get("expires_at")
    org_id = cache.get("org_id")
    username = cache.get("username")
    hostname = cache.get("hostname")
    scheme = cache.get("scheme", "https")
    method = cache.get("method", "username")

    user_dict = {
        "hostname": hostname,
        "scheme": scheme,
        "profile_name": load_ctx.name,
        "profile_config_file": load_ctx.config_file,
        "auth": {
            "token": token,
            "token_type": token_type,
            "expires_at": expires_at,
            "org_id": org_id,
            "username": username,
            "method": method,
        },
    }

    return ProfileUserContext.from_flat_dict(user_dict)


def _compose_profile_cache_filename(load_ctx: ProfileLoadContext) -> Path:
    if load_ctx.config_file:
        return Path(load_ctx.config_file).parent / load_ctx.name
    return HDX_CONFIG_DIR / load_ctx.name


def _try_load_profile_cache_data(load_ctx: ProfileLoadContext) -> CacheDict:
    fname = None
    try:
        with open(
            (inner_fname := _compose_profile_cache_filename(load_ctx)), "r", encoding="utf-8"
        ) as stream:
            fname = inner_fname
            return CacheDict.build_from_toml_stream(stream)
    except FileNotFoundError as ex:
        raise CacheFileNotFoundException(f"Cache file not found {fname}") from ex


def fail_if_token_expired(user_context: ProfileUserContext) -> ProfileUserContext:
    if user_context.token_expiration <= datetime.now():
        raise TokenExpiredException()
    return user_context
