from pathlib import Path
from functools import wraps
import pickle
import atexit
import sys

from ..common.exceptions import HdxCliException


def report_error_and_exit(exctype=Exception, exit_code=-1):
    def report_deco(func):
        @wraps(func)
        def report_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exctype as exc:
                print(f'Error: {exc}')
                sys.exit(exit_code)
        return report_wrapper
    return report_deco


def dynamic_confirmation_prompt(prompt, confirmation_message, fail_message,
                                *,
                                prompt_active=False):
    if not prompt_active:
        return
    the_input = input(prompt)
    if the_input != confirmation_message:
        raise HdxCliException(fail_message)


def confirmation_prompt(prompt, confirmation_message,
                        fail_message):
    def confirmation_prompt_deco(func):
        @wraps(func)
        def confirm_wrapper(*args, **kwargs):
            dynamic_confirmation_prompt(prompt, confirmation_message,
                                        fail_message, prompt_active=True)
            return func(*args, **kwargs)
        return confirm_wrapper
    return confirmation_prompt_deco


_CACHE_DICT = None

def _save_cache(cache_file_path):
    global _CACHE_DICT
    with open(cache_file_path, 'wb') as cache_file:
        pickle.dump(_CACHE_DICT, cache_file)


def _load_cache(cache_file):
    global _CACHE_DICT
    try:
        with open(cache_file, 'rb') as f:
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
        def find_in_disk_cache_deco(user_ctx,
                                    resource_key):
            global _CACHE_DICT
            key_to_find = f'{user_ctx.profilename}.{namespace}.{resource_key}'
            if not _CACHE_DICT:
                _load_cache(cache_file)
                atexit.register(_save_cache, cache_file)
            # Entry found in cache
            if value := _CACHE_DICT.get(key_to_find):
                cache_hits = _CACHE_DICT.setdefault('_hits', 0)
                cache_hits += 1
                _CACHE_DICT['_hits'] = cache_hits
                return value
            # Entry not found
            value = func(user_ctx, resource_key)
            cache_misses = _CACHE_DICT.setdefault('_misses', 0)
            cache_misses += 1
            _CACHE_DICT['_misses'] = cache_misses
            _CACHE_DICT[key_to_find] = value
            return value
        return find_in_disk_cache_deco
    return find_in_disk_cache_wrapper
