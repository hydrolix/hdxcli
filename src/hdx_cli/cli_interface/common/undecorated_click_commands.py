from typing import Optional, List, Tuple, Dict, Any

import json
import click

from hdx_cli.library_api.common.context import ProfileUserContext
from hdx_cli.library_api.common.exceptions import LogicException, HdxCliException


from .cached_operations import * #pylint:disable=wildcard-import,unused-wildcard-import
from ...library_api.common import rest_operations as rest_ops

_MAX_TIMEOUT = 30

def basic_create(profile,
                 resource_path,
                 resource_name: str,
                 body_from_file: Optional[str]=None,
                 body_from_file_type='json',
                 timeout=_MAX_TIMEOUT):
    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    body = {}
    body_stream = None
    if body_from_file:
        # This parameter is for dictionaries
        if body_from_file_type == 'json':
            with open(body_from_file, 'r', encoding='utf-8') as input_body:
                body = json.load(input_body)
                body['name'] = f'{resource_name}'
        else:
            body_stream = open(body_from_file, 'rb')  # pylint:disable=consider-using-with
    else:
        body = {'name': f'{resource_name}',
                'description': 'Created with hdxcli tool'}
    if body_from_file_type == 'json':
        rest_ops.create(url, body=body, headers=headers)
    elif body_from_file and body_stream:
        rest_ops.create_file(url, headers=headers, file_stream=body_stream,
                             remote_filename=resource_name, timeout=timeout)
        if body_stream:
            body_stream.close()
    else:
        rest_ops.create(url, body=body, headers=headers)


def basic_create_with_body_from_string(profile,
                                       resource_path,
                                       resource_name: str,
                                       body_from_string: Optional[str],
                                       body_from_string_type='json'):
    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth

    body = None
    headers = {}
    if body_from_string_type != 'json':
        headers = {'Authorization': f'{token.token_type} {token.token}',
                   # This is basically hardcoding. Could be better
                   'Content-Type': f'application/CSV',
                   'Accept': '*/*'}
        body = body_from_string
    else:
        headers = {'Authorization': f'{token.token_type} {token.token}',
                   'Accept': 'application/json'}
        body = json.loads(body_from_string)
        body['name'] = f'{resource_name}'
    rest_ops.create(url, body=body, headers=headers, body_type=body_from_string_type)



def basic_show(profile, resource_path, resource_name):
    hostname = profile.hostname
    scheme = profile.scheme
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth_info : AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    for resource in resources:
        if resource['name'] == resource_name:
            return json.dumps(resource)


def basic_transform(ctx: click.Context):
    profile_info: ProfileUserContext = ctx.obj['usercontext']
    project_name, table_name = profile_info.projectname, profile_info.tablename
    if not project_name:
        raise HdxCliException("Error. No project name provided and no 'projectname' set in profile")
    hostname = profile_info.hostname
    org_id = profile_info.org_id
    scheme = profile_info.scheme
    list_projects_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/'
    token = profile_info.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    projects_list = rest_ops.list(list_projects_url,
                                  headers=headers)

    try:
        project_id = [p['uuid'] for p in projects_list if p['name'] == project_name][0]

        list_tables_url = f'{scheme}://{hostname}/config/v1/orgs/{org_id}/projects/{project_id}/tables'
        tables_list = rest_ops.list(list_tables_url,
                                    headers=headers)
        table_id = [t['uuid'] for t in tables_list if t['name'] == table_name][0]

        transforms_path = f'/config/v1/orgs/{org_id}/projects/{project_id}/tables/{table_id}/transforms/'
        transforms_url = f'{scheme}://{hostname}{transforms_path}'

        transforms_list = rest_ops.list(transforms_url,
                                        headers=headers)
    except IndexError as idx_err:
        raise LogicException('Cannot find resource.') from idx_err

    if not profile_info.transformname:
        try:
            transform_name = [t['name'] for t in transforms_list if t['settings']['is_default']][0]
            profile_info.transformname = transform_name
        except:
            pass
    else:
        try:
            transform_name = [t['name'] for t in transforms_list if t['name'] == profile_info.transformname][0]
            profile_info.transformname = transform_name
        except IndexError as ex:
            raise TransformNotFoundException(f'Transform not found: {profile_info.transformname}') from ex
    ctx.obj = {'resource_path':
               transforms_path,
               'usercontext': profile_info}


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
        if isinstance(elem, KeyAbsent):
            return "(Key absent)"
        return json.dumps(elem)


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
    elif plural == 'kinesis':
        return 'kinesis', 'kinesis'
    singular = plural if not plural.endswith('s') else plural[0:-1]
    return plural, singular


def _cleanup_some_fields_when_updateworkaround(body_dict):
    if ((settings := body_dict.get('settings')) and
        (autoingest := settings.get('autoingest')) and
         not autoingest[0]['enabled']):
        del body_dict['settings']['autoingest']
    return body_dict


DottedKey = str


def _settings_update(resource: Dict[str, Any],
                     key: DottedKey,
                     value: Any):
    "Update resource and return it with updated_data"
    key_parts = key.split('.')
    if len(key_parts) == 1:
        resource[key_parts[0]] = json.loads(value)
        return resource
    the_value = None
    try:
        the_value = json.loads(value)
    except json.JSONDecodeError:
        the_value = value
    resource_key = resource[key_parts[0]]
    for k in key_parts[1:-1]:
        resource_key = resource_key[k]

    resource_key[key_parts[-1]] = the_value
    return resource


def basic_settings(profile,
                   resource_path,
                   key,
                   value):
    """Given a resource type, it returns the settings that can be used for it"""
    hostname = profile.hostname
    scheme = profile.scheme
    settings_url = f"{scheme}://{hostname}{resource_path}"
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
        resource = [r for r in resources if r["name"] == getattr(profile, resource_kind + "name")][0]
    except IndexError as idx_err:
        raise LogicException('Cannot find resource.') from idx_err

    if not key:
        # project_str = f'Project: {profile.projectname}'
        print(f'{"-" * (90 + 30 + 40)}')
        print(_format_settings_header([("name", 90), ("type", 30), ("value", 40)]))
        _for_each_setting(options, resource=resource)
    elif key and not value:
        try:
            print(f"{key}: {_get_dotted_key_from_dict(key, resource)}")
        except KeyError as ke:
            print(f'Key not found in {resource["name"]}: {key}')
    else:
        try:
            this_resource_url = f'{settings_url}{resource["uuid"]}'
            resource = _settings_update(resource, key, value)
            rest_ops.update_with_put(
                this_resource_url,
                headers=headers,
                body=resource)
        except:
            patch_data = _create_dict_from_dotted_key_and_value(key, value)
            this_resource_url = f'{settings_url}{resource["uuid"]}'
            rest_ops.update_with_patch(
                this_resource_url,
                headers=headers,
                body=patch_data)
        print(f'Updated {resource["name"]} {key}')


def basic_delete(profile, resource_path, resource_name: str):
    hostname = profile.hostname
    scheme = profile.scheme
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth = profile.auth
    headers = {'Authorization': f'{auth.token_type} {auth.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    url = None
    for a_resource in resources:
        if a_resource['name'] == resource_name:
            if 'url' in a_resource:
                url = a_resource['url'].replace('https://', f'{scheme}://')
            else:
                url = f"{scheme}://{hostname}{resource_path}{a_resource['uuid']}"
            break
    if not url:
        return False
    else:
        rest_ops.delete(url, headers=headers)
        return True


def basic_list(profile, resource_path):
    hostname = profile.hostname
    scheme = profile.scheme
    list_url = f'{scheme}://{hostname}{resource_path}'
    auth_info: AuthInfo = profile.auth
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}
    resources = rest_ops.list(list_url, headers=headers)
    for resource in resources:
        if isinstance(resource, str):
            print(resource)
        else:
            print(resource['name'], end='')
            if (settings := resource.get('settings')) and settings.get('is_default'):
                print(' (default)', end='')
            print()
