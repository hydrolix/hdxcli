import json
import requests

from ...library_api.common.exceptions import HdxCliException
from ...library_api.common.context import ProfileUserContext
from ...library_api.utility.decorators import find_in_disk_cache
from ...library_api.common.config_constants import HDX_CLI_HOME_DIR
from ...library_api.common.generic_resource import access_resource


def find_kafka(user_ctx: ProfileUserContext):
    return access_resource(user_ctx,
                    [('projects', user_ctx.projectname),
                     ('tables', user_ctx.tablename),
                     ('sources/kafka', None)])

def find_kinesis(user_ctx: ProfileUserContext):
    return access_resource(user_ctx,
                    [('projects', user_ctx.projectname),
                     ('tables', user_ctx.tablename),
                     ('sources/kinesis', None)])


def find_projects(user_ctx: ProfileUserContext):
    token = user_ctx.auth
    hostname = user_ctx.hostname
    scheme = user_ctx.scheme
    url = f"{scheme}://{hostname}/config/v1/orgs/{user_ctx.org_id}/projects/"
    headers = {"Authorization": f"{token.token_type} {token.token}",
               "Accept": "application/json"}
    result = requests.get(url, headers=headers, timeout=30)
    if result.status_code != 200:
        raise HdxCliException(f"Error getting projects.")
    return json.loads(result.content)


def find_batch(user_ctx: ProfileUserContext):
    token = user_ctx.auth
    hostname = user_ctx.hostname
    scheme = user_ctx.scheme
    url = f"{scheme}://{hostname}/config/v1/orgs/{user_ctx.org_id}/jobs/batch/"
    headers = {"Authorization": f"{token.token_type} {token.token}",
               "Accept": "application/json"}
    result = requests.get(url, headers=headers, timeout=30)
    if result.status_code != 200:
        raise HdxCliException("Error getting projects.")
    return json.loads(result.content)


@find_in_disk_cache(cache_file=HDX_CLI_HOME_DIR / "cache/cache.bin",
                    namespace="projects_ids")
def find_project_id(user_ctx, project_name):
    projects = find_projects(user_ctx)
    return [t["uuid"] for t in projects if t["name"] == project_name]


def _find_project_resource(user_ctx: ProfileUserContext, resource):
    """resource parameter is 'functions' or 'tables', 'dictionaries' or anything project-level"""
    project_id = [ p for p in find_projects(user_ctx)
                    if p["name"] == user_ctx.projectname][0]["uuid"]
    token = user_ctx.auth
    hostname = user_ctx.hostname
    scheme = user_ctx.scheme
    url = f"{scheme}://{hostname}/config/v1/orgs/{user_ctx.org_id}/projects/{project_id}/{resource}"
    headers={"Authorization": f"{token.token_type} {token.token}",
             "Accept": "application/json"}
    result = requests.get(url, headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f"Error getting projects.")
    return json.loads(result.content)


def find_tables(user_ctx: ProfileUserContext):
    return _find_project_resource(user_ctx, 'tables')

def find_dictionaries(user_ctx: ProfileUserContext):
    return _find_project_resource(user_ctx, 'dictionaries')

def find_functions(user_ctx: ProfileUserContext):
    return _find_project_resource(user_ctx, 'functions')


@find_in_disk_cache(cache_file=HDX_CLI_HOME_DIR / "cache/cache.bin",
                    namespace="tables_ids")
def find_table_id(user_ctx, table_name):
    tables = find_tables(user_ctx)
    return [t["uuid"] for t in tables if t["name"] == table_name]


def find_transforms(user_ctx: ProfileUserContext):
    project_id = [ p for p in find_projects(user_ctx)
                    if p["name"] == user_ctx.projectname][0]["uuid"]
    table_id = [ p for p in find_tables(user_ctx)
                     if p["name"] == user_ctx.tablename][0]["uuid"]
    token = user_ctx.auth
    hostname = user_ctx.hostname
    scheme = user_ctx.scheme
    url = f"{scheme}://{hostname}/config/v1/orgs/{user_ctx.org_id}/projects/{project_id}/tables/{table_id}/transforms"
    headers={"Authorization": f"{token.token_type} {token.token}",
             "Accept": "application/json"}
    result = requests.get(url, headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f"Error getting projects.")
    return json.loads(result.content)


@find_in_disk_cache(cache_file=HDX_CLI_HOME_DIR / "cache/cache.bin",
                    namespace="transforms_ids")
def find_transform_id(user_ctx, transform_name):
    transforms = find_transforms(user_ctx)
    return [t["uuid"] for t in transforms if t["name"] == transform_name]


