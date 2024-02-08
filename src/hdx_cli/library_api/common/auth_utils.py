import os
import tempfile
from datetime import datetime
import dataclasses as dc

import functools as ft
from pathlib import Path

import toml

from .auth import load_profile, try_load_profile_from_cache_data, save_profile_cache
from .context import ProfileUserContext, ProfileLoadContext, DEFAULT_TIMEOUT
from .exceptions import TokenExpiredException, HdxCliException
from .login import login
from .config_constants import HDX_CONFIG_DIR


def load_user_context(load_context, **args):
    load_set_params = ft.partial(_load_set_config_parameters,
                                 load_context=load_context)

    user_context = _chain_calls_ignore_exc(try_load_profile_from_cache_data,
                                           fail_if_token_expired,
                                           load_set_params,
                                           # Parameters to first function
                                           load_ctx=load_context,
                                           # _chain_calls_ignore_exc Function configuration
                                           exctype=HdxCliException)
    if not user_context:
        user_context: ProfileUserContext = load_profile(load_context)
        auth_info = login(user_context.username,
                          user_context.hostname,
                          password=args.get('password'),
                          use_ssl=user_context.scheme == 'https')
        user_context.auth = auth_info
        user_context.org_id = auth_info.org_id
        cache_dir_path = (Path(args.get('profile_config_file')).parent
                          if args.get('profile_config_file') else HDX_CONFIG_DIR)
        save_profile_cache(user_context,
                           token=auth_info.token,
                           expiration_time=auth_info.expires_at,
                           token_type=auth_info.token_type,
                           org_id=auth_info.org_id,
                           cache_dir_path=cache_dir_path)
        user_context.auth = auth_info

    uri_scheme = args.get('uri_scheme')
    timeout = args.get('timeout')
    if uri_scheme and uri_scheme != 'default':
        user_context.scheme = args.get('uri_scheme')
    if timeout and timeout != DEFAULT_TIMEOUT:
        user_context.timeout = args.get('timeout')

    return user_context


def generate_temporal_profile(cluster_hostname,
                              cluster_username,
                              cluster_password,
                              cluster_uri_scheme):
    target_profiles_file = Path(tempfile.gettempdir() + os.sep +
                                cluster_username + '_' +
                                cluster_hostname + '.toml')
    _setup_target_cluster_config(target_profiles_file,
                                 cluster_username,
                                 cluster_hostname,
                                 cluster_uri_scheme)
    target_load_ctx = ProfileLoadContext('default', target_profiles_file)
    auth_info = login(cluster_username,
                      cluster_hostname,
                      password=cluster_password,
                      use_ssl=(cluster_uri_scheme == 'https'))
    temp_profile = load_profile(target_load_ctx)
    temp_profile.auth = auth_info
    temp_profile.org_id = auth_info.org_id

    save_profile_cache(temp_profile,
                       token=temp_profile.auth.token,
                       org_id=temp_profile.org_id,
                       token_type='Bearer',
                       expiration_time=temp_profile.auth.expires_at,
                       cache_dir_path=temp_profile.profile_config_file.parent)
    return temp_profile


def _setup_target_cluster_config(profile_config_file,
                                 target_cluster_username,
                                 target_cluster_hostname,
                                 target_cluster_scheme):
    username = target_cluster_username
    hostname = target_cluster_hostname
    scheme = target_cluster_scheme
    config_data = {'default': {'username': username, 'hostname': hostname, 'scheme': scheme}}
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(config_data, config_file)


def _chain_calls_ignore_exc(*funcs, **kwargs):
    """
    Chain calls and return on_error_return on failure. Exceptions are considered failure if
    they derived from provided kwarg 'exctype', otherwise they will escape.
    on_error_default kwarg is what is returned in case of failure.

    Function parameters to funcs[0] are provided in kwargs, and subsequent results
    are provided as input of the first function.
    """
    # Setup function
    exctype = kwargs.get('exctype', Exception)
    on_error_return = kwargs.get('on_error_return', None)
    try:
        del kwargs['exctype']
    except: # pylint:disable=bare-except
        pass
    try:
        del kwargs['on_error_return']
    except: # pylint:disable=bare-except
        pass

    # Run functions
    try:
        result = funcs[0](**kwargs)
        if len(funcs) > 1:
            for func in funcs[1:]:
                result = func(result)
        return result
    except exctype:
        return on_error_return


def _load_set_config_parameters(user_context: ProfileUserContext,
                                load_context: ProfileLoadContext):
    """Given a profile to load and an old profile, it returns the user_context
    with the config parameters projectname and tablename loaded."""
    config_params = {'projectname':
                     (prof := load_profile(load_context)).projectname,
                     'tablename':
                     prof.tablename,
                     'scheme': prof.scheme}
    user_ctx_dict = dc.asdict(user_context) | config_params
    # Keep old auth since asdict will transform AuthInfo into a dictionary.
    old_auth = user_context.auth
    new_user_ctx = ProfileUserContext(**user_ctx_dict)
    # And reassign when done
    new_user_ctx.auth = old_auth
    return new_user_ctx


def fail_if_token_expired(user_context: ProfileUserContext):
    if user_context.auth.expires_at <= datetime.now():
        raise TokenExpiredException()
    return user_context
