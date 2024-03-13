import click
import json
from typing import Union, List, Tuple

from ...library_api.common.auth import AuthInfo
from ...library_api.common.exceptions import QueryOptionNotFound, HdxCliException
from ...library_api.common.logging import get_logger
from ...library_api.common import rest_operations as rest_ops
from ...library_api.utility.decorators import report_error_and_exit

logger = get_logger()


@click.group(help='Query options operations at org-level', name='query-option')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def query_option(ctx: click.Context):
    profile = ctx.parent.obj['usercontext']
    org_id = profile.org_id
    ctx.obj = {'resource_path': f'/config/v1/orgs/{org_id}/query_options/',
               'usercontext': profile}


@click.command(help='Set query option(s).', name='set')
@click.argument('query_option_name', default=None, required=False)
@click.argument('query_option_value', default=None, required=False)
@click.option('--from-file', default=None,
              help='Set query options from a JSON file.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def set_(ctx: click.Context,
         query_option_name: str,
         query_option_value: Union[str, int],
         from_file):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']

    if not (query_option_name and query_option_value) and not from_file:
        raise click.BadParameter(
            'You must provide either query_option_name and query_option_value or --from-file (JSON).')

    response = _set(user_profile, resource_path, query_option_name, query_option_value, from_file)
    logger.info(f'{response}')


@click.command(help='Unset query option(s).')
@click.argument('query_option_name', default=None, required=False)
@click.option('--all', 'all_query_options', is_flag=True, default=False,
              help='Unset all query options.')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def unset(ctx: click.Context,
          query_option_name: str,
          all_query_options: bool):
    user_profile = ctx.parent.obj['usercontext']
    resource_path = ctx.parent.obj['resource_path']

    if query_option_name is None and not all_query_options:
        raise click.BadParameter('Either provide a QUERY_OPTION_NAME or --all option.')

    response = _unset(user_profile, resource_path, query_option_name=query_option_name)
    logger.info(f'{response}')


@click.command(help='List query options.', name='list')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    resource_path = ctx.parent.obj['resource_path']
    profile = ctx.parent.obj['usercontext']
    _list(profile, resource_path)


def _set(profile, resource_path, query_option_name=None, query_option_value=None, from_file=None):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}

    if not (available_options := _available_query_options(url, headers, timeout)):
        logger.error("There was an error catching available query options.")
        return

    result = rest_ops.list(url, headers=headers, timeout=timeout)
    if not result.get('settings') or not ('default_query_options' in result.get('settings')):
        raise HdxCliException('An error occurred while trying to get the query options.')

    if query_option_name:
        if query_option_name not in available_options:
            raise QueryOptionNotFound(f"'{query_option_name}' is not a valid query option.")

        result['settings']['default_query_options'][query_option_name] = query_option_value
    else:
        try:
            with open(from_file, 'r') as file:
                query_options_from_file = json.load(file)
        except FileNotFoundError:
            raise HdxCliException('The specified file does not exist.')
        except json.JSONDecodeError:
            raise HdxCliException('The file does not contain valid JSON.')

        if not all(key in available_options for key in query_options_from_file.keys()):
            raise QueryOptionNotFound(f'There are invalid query options in the file.')

        result['settings']['default_query_options'].update(query_options_from_file)

    rest_ops.update_with_put(url,
                             headers=headers,
                             body=result,
                             timeout=timeout,
                             params=None)
    return f"Set '{query_option_name}' query option" if query_option_name else f'Set query options from file {from_file}'


def _unset(profile, resource_path, query_option_name=None):
    hostname = profile.hostname
    scheme = profile.scheme
    timeout = profile.timeout
    url = f'{scheme}://{hostname}{resource_path}'
    token = profile.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}

    result = rest_ops.list(url, headers=headers, timeout=timeout)

    if not result.get('settings') or not ('default_query_options' in result.get('settings')):
        raise HdxCliException('An error occurred while trying to get the query options.')

    data = result['settings']
    try:
        if query_option_name:
            del data['default_query_options'][query_option_name]
        else:
            del data['default_query_options']
    except KeyError as key_err:
        raise QueryOptionNotFound(f'{query_option_name} not found in the set query options.') from key_err

    rest_ops.update_with_put(url,
                             headers=headers,
                             body=result,
                             timeout=timeout,
                             params=None)
    return f"Unset '{query_option_name}' query option" if query_option_name else 'Unset all query options'


def _list(profile, resource_path):
    hostname = profile.hostname
    scheme = profile.scheme
    url = f'{scheme}://{hostname}{resource_path}'
    auth_info: AuthInfo = profile.auth
    timeout = profile.timeout
    headers = {'Authorization': f'{auth_info.token_type} {auth_info.token}',
               'Accept': 'application/json'}

    default_query_options = (rest_ops.list(url, headers=headers, timeout=timeout)
                             .get('settings', {})
                             .get('default_query_options'))
    if not default_query_options:
        return

    if not (available_options := _available_query_options(url, headers, timeout)):
        logger.error("There was an error catching available query options.")
        return

    logger.info(f'{"-" * (55 + 15 + 25)}')
    logger.info(_format_settings_header([("name", 55), ("type", 15), ("value", 25)]))
    logger.info(f'{"-" * (55 + 15 + 25)}')
    for setting_name, setting_val in available_options.items():
        if default_query_options.get(setting_name) is not None:
            logger.info(f"{setting_name:<55}{setting_val['type']:<15}{default_query_options[setting_name]:<25}")


def _available_query_options(url, headers, timeout) -> dict:
    response = rest_ops.options(url, headers=headers, timeout=timeout)
    actions = response.get('actions', {})
    put_action = actions.get('PUT', {})
    settings = put_action.get('settings', {})
    children = settings.get('children', {})
    return children.get('default_query_options', {}).get('children')


def _format_settings_header(headers_and_spacing: List[Tuple[str, int]]):
    format_strings = []
    for key, spacing in headers_and_spacing:
        format_strings.append(f"{key:<{spacing}}")
    return "".join(format_strings)


query_option.add_command(set_)
query_option.add_command(list_)
query_option.add_command(unset)
