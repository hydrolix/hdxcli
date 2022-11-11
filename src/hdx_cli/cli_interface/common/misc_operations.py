from typing import List, Tuple

import click

from ...library_api.common.exceptions import HdxCliException, LogicException
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit

from .cached_operations import * #pylint:disable=wildcard-import,unused-wildcard-import


class KeyAbsent:
    """Show absent key into the settings output"""
    def __str__(self):
        return "(Key absent)"


def _get_dotted_key_from_dict(dotted_key, the_dict):
    key_path = dotted_key.split(".")
    val = the_dict[key_path[0]]
    if len(key_path) > 1:
        for key_piece in key_path[1:]:
            val = val[key_piece]
    return val


def _do_create_dict_from_dotted_key_and_value(split_key, value,
                                              the_dict):
    if len(split_key) == 1:
        the_dict[split_key[0]] = value
        return
    else:
        the_dict[split_key[0]] = {}
        _do_create_dict_from_dotted_key_and_value(split_key[1:],
                                                  value,
                                                  the_dict[split_key[0]])

def _create_dict_from_dotted_key_and_value(dotted_key, value):
    the_dict = {}
    split_key = dotted_key.split(".")
    if len(split_key) == 1:
        return {dotted_key: value}
    _do_create_dict_from_dotted_key_and_value(split_key,
                                              value,
                                              the_dict)
    return the_dict


def _wrap_str(contents, prefix, suffix):
    return prefix + contents + suffix


def _format_key_val(key: str, val):
    return f"{key}:{_format_elem(val, obj_detailed=False)}"


def _format_list(lst, nelems=5):
    max_index = min(nelems, len(lst))
    result = []
    for val in lst[0:max_index]:
        result.append(_format_elem(val, obj_detailed=False))
    if max_index < len(lst):
        result.append("...")
    return _wrap_str(", ".join(result), "[", f"] ({len(lst)} elements)")


def _format_dict(dic, nelems=4, detailed=True):
    if not detailed:
        return "{...}"
    else:
        sorted_elems = sorted(dic.items())
        max_index = min(nelems, len(sorted_elems))
        result = []
        for key, val in sorted_elems[0:max_index]:
            result.append(_format_key_val(key, val))
        if max_index < len(sorted_elems):
            result.append("...")
        return _wrap_str(
            ", ".join(result), "{", f"}} ({len(sorted_elems)} keys)")


def _format_elem(elem, obj_detailed=True):
    if isinstance(elem, list):
        return _format_list(elem)
    elif isinstance(elem, dict):
        return _format_dict(elem, detailed=obj_detailed)
    else:
        if isinstance(elem, str):
            return f"'{str(elem)}'"
        return str(elem)

def _format_setting(dotted_key, value, resource_value):
    return f"{dotted_key:<90}{value:<30}{_format_elem(resource_value):<40}"


def _format_settings_header(headers_and_spacing: List[Tuple[str, int]]):
    format_strings = []
    for key, spacing in headers_and_spacing:
        format_strings.append(f"{key:<{spacing}}")
    format_strings.append("\n")
    format_strings.append(
        "-" * sum((header[1] for header in headers_and_spacing)))
    return "".join(format_strings)


def _do_for_each_setting(settings_dict, prefix="",
                      resource=None):
    for setting_name, setting_val in settings_dict.items():
        if setting_val["read_only"]:
            continue
        if setting_val["type"] == "nested object":
            the_prefix = prefix + "." if prefix else ""
            _for_each_setting(setting_val["children"],
                              the_prefix + setting_name,
                              resource)
        else:
            full_key_name = (setting_name if not prefix
                             else prefix + "." + setting_name)
            the_value_in_resource = None
            try:
                the_value_in_resource = _get_dotted_key_from_dict(full_key_name, resource)
            except KeyError:
                the_value_in_resource = KeyAbsent()
            print(_format_setting(full_key_name, setting_val["type"],
                                  the_value_in_resource))


def _for_each_setting(settings_dict, prefix="",
                         resource=None):
    _do_for_each_setting(settings_dict, prefix, resource)


def _heuristically_get_resource_kind(resource_path) -> Tuple[str, str]:
    """Returns plural and singular names for resource kind given a resource path.
       If it is a nested resource
    For example:

          - /config/.../tables/ -> ('tables', 'table')
          - /config/.../projects/ -> ('projects', 'project')
          - /config/.../jobs/batch/ -> ('batch', 'batch')
    """
    split_path = resource_path.split("/")
    plural = split_path[-2]
    if plural == "dictionaries":
        return "dictionaries", "dictionary"
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular


def _cleanup_some_fields_when_updateworkaround(body_dict):
    if ((settings := body_dict.get('settings')) and
        (autoingest := settings.get('autoingest')) and
         not autoingest[0]['enabled']):
        del body_dict['settings']['autoingest']
    return body_dict


@click.command(help="Get, set or list settings on a resource. When invoked with "
               "only the key, it retrieves the value of the setting. If retrieved "
               "with both key and value, the value for the key, if it exists, will "
               "be set.\n"
               "Otherwise, when invoked with no arguments, all the settings will be listed.")
@click.argument("key", required=False, default=None)
@click.argument("value", required=False, default=None)
@click.option("--body-from-file", "-f",
              help="Create uses as body for request the file contents. "
              "name key from the body will be replaced by the given 'resource_name'.")
@click.pass_context
@report_error_and_exit(exctype=HdxCliException)
def settings(ctx: click.Context,
             key,
             value,
             body_from_file):
    """Given a resource type, it returns the settings that can be used for it"""
    resource_path = ctx.parent.obj["resource_path"]
    # Resource kind is extracted from the url pattern
    profile = ctx.parent.obj["usercontext"]
    hostname = profile.hostname
    settings_url = f"https://{hostname}{resource_path}"
    auth = profile.auth
    headers = {"Authorization": f"{auth.token_type} {auth.token}",
               "Accept": "application/json"}
    options = rest_ops.options(settings_url, headers=headers)["actions"]["POST"]

    resource_kind_plural, resource_kind = (
        _heuristically_get_resource_kind(resource_path))

    if not getattr(profile, resource_kind + "name"):
        raise LogicException(f'No default {resource_kind} found in profile')
    resources = None
    try:
        resources = globals()["find_" + resource_kind_plural](profile)
        resource = [ r for r in resources if r["name"] == getattr(profile, resource_kind + "name")][0]
    except IndexError as idx_err:
        raise LogicException(f'Cannot find resource.') from idx_err

    if not key:
        project_str = f'Project: {profile.projectname}'
        print(project_str)
        print(f'{"-" * (90 + 30 + 40)}')
        print(_format_settings_header([("name", 90), ("type", 30), ("value", 40)]))
        _for_each_setting(options, resource=resource)
    elif key and not value:
        try:
            print(f"{key}: {_get_dotted_key_from_dict(key, resource)}")
        except KeyError as ke:
            print(f'Key not found in {resource["name"]}: {key}')
    elif key and body_from_file:
        with open(body_from_file, "r") as input_body:
            try:
                patch_data = _create_dict_from_dotted_key_and_value(key, value)
                this_resource_url = f'{settings_url}{resource["uuid"]}'
                rest_ops.update_with_patch(
                    this_resource_url,
                    headers=headers,
                    body=patch_data)
            except:
                put_data = _create_dict_from_dotted_key_and_value(key, value)
                this_resource_url = f'{settings_url}{resource["uuid"]}'
                updated_resource = resource
                updated_resource.update(put_data)
                rest_ops.update_with_put(
                this_resource_url,
                headers=headers,
                body=updated_resource)
            print(f'Updated {resource["name"]} {key}')
    else:
        try:
            patch_data = _create_dict_from_dotted_key_and_value(key, value)
            this_resource_url = f'{settings_url}{resource["uuid"]}'
            rest_ops.update_with_patch(
                this_resource_url,
                headers=headers,
                body=patch_data)
        except:
            put_data = _create_dict_from_dotted_key_and_value(key, value)
            this_resource_url = f'{settings_url}{resource["uuid"]}'
            updated_resource = resource
            updated_resource.update(put_data)
            # updated_resource = _cleanup_auto_ingest_workaround(updated_resource)
            rest_ops.update_with_put(
                this_resource_url,
                headers=headers,
                body=updated_resource)
        print(f'Updated {resource["name"]} {key}')
