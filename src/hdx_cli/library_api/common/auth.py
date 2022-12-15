from datetime import datetime
from pathlib import Path

import os
from typing import overload, Union

import toml

from .config_constants import HDX_CLI_HOME_DIR, PROFILE_CONFIG_FILE
from .exceptions import ProfileNotFoundException, CacheFileNotFoundException, LogicException
from .context import ProfileUserContext, ProfileLoadContext
from .cache import CacheDict
from ..userdata.token import AuthInfo


__all__ = ['load_profile', 'try_load_profile_from_cache_data', 'save_profile_cache']


@overload
def load_profile(load_profile_context:
                 ProfileLoadContext) -> ProfileUserContext: ...

@overload
def load_profile(load_profile_name: str) -> ProfileUserContext: ...


def load_profile(load_profile_context:
                 Union[ProfileLoadContext, str]) -> ProfileUserContext:
    """Loads a profile from a path in disk or from a load context in memory"""
    try:
        profile_config_file, profile_name = None, None
        if isinstance(load_profile_context, ProfileLoadContext):
            profile_config_file = (load_profile_context.profile_config_file
                                   if load_profile_context.profile_config_file
                                   else PROFILE_CONFIG_FILE)
            profile_name = load_profile_context.profilename
        elif isinstance(load_profile_context, str):
            profile_config_file = PROFILE_CONFIG_FILE
            profile_name = load_profile_context
        else:
            raise LogicException('Wrong profile type.')
        with open(profile_config_file, 'r', encoding='utf-8') as stream:
            profile_dict = toml.load(stream)[profile_name]
            profile_dict['profilename'] = profile_name
            profile_dict['profile_config_file'] = profile_config_file
            return ProfileUserContext(**profile_dict)
    except FileNotFoundError as ex:
        raise ProfileNotFoundException(
            f'File not found: {profile_config_file}') from ex
    except KeyError as key_err:
        raise ProfileNotFoundException(
            f'Profile name not found: {profile_name}') from key_err


def save_profile_cache(a_profile: ProfileUserContext,
                       *,
                       token,
                       expiration_time: datetime,
                       org_id,
                       token_type,
                       cache_dir_path=None):
    """
    Save a cache file for this profile.
    The profile cache file is saved in cache_dir_path
    """
    os.makedirs(cache_dir_path, mode=0o700, exist_ok=True)
    username = a_profile.username
    hostname = a_profile.hostname
    with open(cache_dir_path / f'{a_profile.profilename}', 'w', encoding='utf-8') as f:
        CacheDict.build_from_dict({'org_id': f'{org_id}',
                                   'token':{'auth_token': token,
                                            'token_type': token_type,
                                            'expires_at': expiration_time},
                                   'username': f'{username}',
                                   'hostname': f'{hostname}'}).save_to_stream(f)


def _compose_profile_cache_filename(load_ctx: ProfileLoadContext) -> Path:
    if load_ctx.profile_config_file:
        return Path(load_ctx.profile_config_file).parent / load_ctx.profilename
    return HDX_CLI_HOME_DIR / load_ctx.profilename


def _try_load_profile_cache_data(load_ctx: ProfileLoadContext) -> CacheDict:
    fname = None
    try:
        with open((inner_fname := _compose_profile_cache_filename(load_ctx)), 'r',
        encoding='utf-8') as stream:
            fname = inner_fname
            return CacheDict.build_from_toml_stream(stream)
    except FileNotFoundError as ex:
        raise CacheFileNotFoundException(f'Cache file not found {fname}') from ex


def try_load_profile_from_cache_data(load_ctx: ProfileLoadContext) -> ProfileUserContext:
    """Load the data from the cache to avoid making a request if possible.
    It searches the token and other additional info to operate such as org_id,
    and other info that might be needed."""
    cache = _try_load_profile_cache_data(load_ctx)
    user_dict = {'username': cache['username'],
                 'hostname': cache['hostname'],
                 'profilename': load_ctx.profilename,
                 'auth': AuthInfo(cache['token']['auth_token'],
                                   cache['token']['expires_at'],
                                  cache['org_id']),
                 'org_id': cache['org_id'],
                 'profile_config_file': load_ctx.profile_config_file}
    if value := cache.get('projectname'):
        user_dict['projectname'] = value
    if value := cache.get('tablename'):
        user_dict['tablename'] = value
    return ProfileUserContext(**user_dict)
