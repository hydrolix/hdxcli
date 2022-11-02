from pathlib import Path
from typing import overload, Union

import toml

from .config_constants import HDX_CLI_HOME_DIR, PROFILE_CONFIG_FILE
from .exceptions import ProfileNotFoundException, CacheFileNotFoundException
from .context import ProfileUserContext, ProfileLoadContext
from .cache import CacheDict
from ..userdata.token import AuthInfo


__all__ = ['load_profile', 'try_load_profile_from_cache_data']



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

        with open(profile_config_file, 'r', encoding='utf-8') as stream:
            profile = toml.load(stream)[profile_name]
            profile['profilename'] = profile_name
            return ProfileUserContext(**profile)
    except FileNotFoundError as ex:
        raise ProfileNotFoundException(
            f'File not found: {profile_config_file}') from ex
    except KeyError as key_err:
        raise ProfileNotFoundException(
            f'Profile name not found: {profile_name}') from key_err



def _compose_profile_cache_filename(load_ctx: ProfileLoadContext) -> Path:
    if load_ctx.profile_config_file:
        return load_ctx.profile_config_file.parent / load_ctx.profilename
    else:
        return HDX_CLI_HOME_DIR / load_ctx.profilename


def _try_load_profile_cache_data(load_ctx: ProfileLoadContext) -> CacheDict:
    try:
        with open((fname := _compose_profile_cache_filename(load_ctx)), 'r',
        encoding='utf-8') as stream:
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
                 'org_id': cache['org_id']}
    if value := cache.get('projectname'):
        user_dict['projectname'] = value
    if value := cache.get('tablename'):
        user_dict['tablename'] = value
    return ProfileUserContext(**user_dict)
