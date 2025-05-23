import atexit
import functools
import pickle
import sys
from functools import wraps
from pathlib import Path

import click
import toml

from ...auth.context_builder import load_user_context
from ...config.paths import PROFILE_CONFIG_FILE
from ...models import ProfileLoadContext
from ..common.exceptions import HdxCliException, HttpException
from ..common.logging import get_logger
from .json_util import http_error_pretty_format

logger = get_logger()


def report_error_and_exit(exctype=Exception, exit_code=-1):
    def report_deco(func):
        @wraps(func)
        def report_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exctype as exc:
                logger.debug(f"{exc}")
                if isinstance(exc, HttpException):
                    logger.error(f"Error: {http_error_pretty_format(exc)}")
                else:
                    logger.error(f"Error: {exc}")
                sys.exit(exit_code)

        return report_wrapper

    return report_deco


def dynamic_confirmation_prompt(prompt, confirmation_message, fail_message, *, prompt_active=False):
    if not prompt_active:
        return
    logger.info(f"{prompt}[!n]")
    the_input = input("")
    if the_input != confirmation_message:
        raise HdxCliException(fail_message)


def confirmation_prompt(prompt, confirmation_message, fail_message):
    def confirmation_prompt_deco(func):
        @wraps(func)
        def confirm_wrapper(*args, **kwargs):
            if not kwargs.get("disable_confirmation_prompt", False):
                dynamic_confirmation_prompt(
                    prompt, confirmation_message, fail_message, prompt_active=True
                )
            return func(*args, **kwargs)

        return confirm_wrapper

    return confirmation_prompt_deco


_CACHE_DICT = None


def _save_cache(cache_file_path):
    global _CACHE_DICT
    with open(cache_file_path, "wb") as cache_file:
        pickle.dump(_CACHE_DICT, cache_file)


def _load_cache(cache_file):
    global _CACHE_DICT
    try:
        with open(cache_file, "rb") as f:
            _CACHE_DICT = pickle.load(f)
    except FileNotFoundError:
        Path(cache_file.parent).mkdir(parents=True, exist_ok=True)
        _CACHE_DICT = {}


def find_in_disk_cache(cache_file, namespace):
    """Find an entry in a disk cache in namespace and profile with key.
    Currently this disk cache is being used with tables,
    transforms, projects, etc. to lower the number of requests to the
    server
    """

    def find_in_disk_cache_wrapper(func):
        def find_in_disk_cache_deco(user_ctx, resource_key):
            global _CACHE_DICT
            key_to_find = f"{user_ctx.profilename}.{namespace}.{resource_key}"
            if not _CACHE_DICT:
                _load_cache(cache_file)
                atexit.register(_save_cache, cache_file)
            # Entry found in cache
            if value := _CACHE_DICT.get(key_to_find):
                cache_hits = _CACHE_DICT.setdefault("_hits", 0)
                cache_hits += 1
                _CACHE_DICT["_hits"] = cache_hits
                return value
            # Entry not found
            value = func(user_ctx, resource_key)
            cache_misses = _CACHE_DICT.setdefault("_misses", 0)
            cache_misses += 1
            _CACHE_DICT["_misses"] = cache_misses
            _CACHE_DICT[key_to_find] = value
            return value

        return find_in_disk_cache_deco

    return find_in_disk_cache_wrapper


def ensure_logged_in(func):
    @wraps(func)
    def decorated_function(ctx: click.Context, *args, **kwargs):
        profile_context = ctx.parent.obj["profilecontext"]
        user_options = ctx.parent.obj["useroptions"]
        user_context = load_user_context(
            profile_context,
            username=user_options.get("username"),
            password=user_options.get("password"),
            profile_config_file=user_options.get("profile_config_file"),
            uri_scheme=user_options.get("uri_scheme"),
            timeout=user_options.get("timeout"),
        )
        ctx.parent.obj["usercontext"] = user_context
        return func(ctx, *args, **kwargs)

    return decorated_function


def with_profiles_context(func):
    @functools.wraps(func)
    def decorated_function(ctx, *args, **kwargs):
        profile_config_file = ctx.parent.obj["profilecontext"].config_file
        profile_configs = get_profile_configs(profile_config_file=profile_config_file)
        profile_context = ProfileLoadContext(
            name=kwargs.get("profile_name"),
            config_file=profile_config_file,
        )
        return func(ctx, profile_context, profile_configs, *args, **kwargs)

    return decorated_function


def skip_group_logic_on_help(func):
    """Decorator to skip group logic if --help is in sys.argv."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        if "--help" in sys.argv:
            return
        return func(*args, **kwargs)

    return wrapper


def force_operation_option(func):
    """Decorator to add the --force-operation option."""
    func = click.option(
        "-F",
        "--force-operation",
        is_flag=True,
        default=False,
        help='This flag allows adding the "force_operation" parameter to the request.',
    )(func)
    return func


def no_rollback_option(func):
    """
    Decorator for handling the '--no-rollback' option.
    If the user provides --no-rollback, then no_rollback == True.
    Otherwise, it remains False by default.
    """
    func = click.option(
        "--no-rollback",
        is_flag=True,
        default=False,
        help="Disable rollback behavior in case of errors.",
    )(func)
    return func


def target_cluster_options(func):
    """
    Decorator that adds all the common target cluster/profile options.
    """
    func = click.option(
        "-tp",
        "--target-profile",
        required=False,
        default=None,
        help="Name of an existing profile to connect to the target host.",
    )(func)

    func = click.option(
        "-h",
        "--target-cluster-hostname",
        required=False,
        default=None,
        help="Hostname of the target cluster.",
    )(func)

    func = click.option(
        "-u",
        "--target-cluster-username",
        required=False,
        default=None,
        help="Username to authenticate to the target cluster.",
    )(func)

    func = click.option(
        "-p",
        "--target-cluster-password",
        required=False,
        default=None,
        help="Password for the target cluster user.",
    )(func)

    func = click.option(
        "-s",
        "--target-cluster-uri-scheme",
        required=False,
        default="https",
        help="Protocol to use (http or https). Defaults to 'https'.",
    )(func)

    return func


def get_profile_configs(profile_config_file: Path = PROFILE_CONFIG_FILE) -> dict:
    with open(profile_config_file, "r", encoding="utf-8") as config_file:
        return toml.load(config_file)
