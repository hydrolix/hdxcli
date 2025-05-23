import json

import click

from ...library_api.common import rest_operations as rest_ops
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from ...models import ProfileLoadContext, ProfileUserContext
from ..common.undecorated_click_commands import basic_create, basic_transform

logger = get_logger()

_RAW_HOSTNAME = "raw.githubusercontent.com"
_REPO_USER = "hydrolix/transforms"
DEFAULT_INDENTATION = 4


@click.group(help="Public resources management")
@click.pass_context
def integration(ctx: click.Context):
    profile_context: ProfileLoadContext = ctx.parent.obj["profilecontext"]
    user_options = ctx.parent.obj["useroptions"]
    ctx.obj = {
        "resource_path": f"/{_REPO_USER}",
        "raw_hostname": f"{_RAW_HOSTNAME}",
        "profilecontext": profile_context,
        "useroptions": user_options,
    }


@click.group(help="Public transforms.")
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLENAME",
    default=None,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def transform(ctx: click.Context, project_name: str, table_name: str):
    user_profile: ProfileUserContext = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, projectname=project_name, tablename=table_name)
    resource_path = ctx.parent.obj["resource_path"]
    resource_path = f"{resource_path}/dev"
    ctx.obj = {
        "resource_path": resource_path,
        "raw_hostname": ctx.parent.obj["raw_hostname"],
        "usercontext": user_profile,
    }


integration.add_command(transform)


def _github_list(ctx: click.Context):
    profile: ProfileUserContext = ctx.parent.obj["usercontext"]
    raw_hostname = ctx.parent.obj["raw_hostname"]
    resource_path = ctx.parent.obj["resource_path"]
    resource_path = f"{resource_path}/index.json"
    url = f"https://{raw_hostname}{resource_path}"
    timeout = profile.timeout
    return rest_ops.get(url, headers={}, timeout=timeout)


@click.command(help="Integration transforms.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    results = _github_list(ctx)
    for obj in results:
        name = obj["name"]
        description = obj["description"]
        vendor = obj["vendor"]
        logger.info(f"{name: <20} {description: <70} from {vendor: <40}")


def _basic_show(ctx: click.Context, transform_name: str, indent: bool = False):
    results = _github_list(ctx)
    user_profile: ProfileUserContext = ctx.parent.obj["usercontext"]
    raw_hostname = ctx.parent.obj["raw_hostname"]
    resource_path = ctx.parent.obj["resource_path"]
    base_resource_url = f"https://{raw_hostname}{resource_path}"
    for res in results:
        if res["name"] == transform_name:
            resource_url = f'{base_resource_url}{res["url"]}'
            timeout = user_profile.timeout
            result = rest_ops.get(resource_url, headers={}, timeout=timeout)
            indentation = DEFAULT_INDENTATION if indent else None
            return json.dumps(result, indent=indentation)
    else:
        raise ValueError(f"No transform named {transform_name}.")


@click.command(help="Apply an integration transform into current table.")
@click.argument("integration_transform_name", metavar="INTEGRATIONTRANSFORMNAME")
@click.argument("transform_name", metavar="TRANSFORMNAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def apply(ctx: click.Context, integration_transform_name: str, transform_name: str):
    transform_contents = _basic_show(ctx, integration_transform_name)
    transform_dict = json.loads(transform_contents)
    try:
        del transform_dict["settings"]["is_default"]
    except KeyError:
        pass

    user_profile = ctx.parent.obj["usercontext"]
    basic_transform(ctx)
    resource_path = ctx.obj["resource_path"]
    basic_create(user_profile, resource_path, transform_name, body=transform_dict)
    logger.info(f"Created transform {transform_name} from {integration_transform_name}")


@click.command(help="Integration transforms.")
@click.argument("transform_name", metavar="TRANSFORMNAME")
@click.option(
    "-i",
    "--indent",
    is_flag=True,
    default=False,
    help="Number of spaces for indentation in the output.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, transform_name: str, indent: bool):
    logger.info(_basic_show(ctx, transform_name, indent=indent))


transform.add_command(list_, name="list")
transform.add_command(apply, "apply")
transform.add_command(show)
