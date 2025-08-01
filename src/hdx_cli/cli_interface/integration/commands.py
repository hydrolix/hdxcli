import json

import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_transform, basic_create
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.library_api.common import rest_operations as rest_ops
from hdx_cli.models import ProfileLoadContext, ProfileUserContext

logger = get_logger()

_RAW_HOSTNAME = "raw.githubusercontent.com"
_REPO_USER = "hydrolix/transforms"
DEFAULT_INDENTATION = 4


@click.group(cls=HdxGroup)
@click.pass_context
def integration(ctx: click.Context):
    """Commands to manage public integration resources."""
    profile_context: ProfileLoadContext = ctx.parent.obj["profilecontext"]
    user_options = ctx.parent.obj["useroptions"]
    ctx.obj = {
        "resource_path": f"/{_REPO_USER}",
        "raw_hostname": f"{_RAW_HOSTNAME}",
        "profilecontext": profile_context,
        "useroptions": user_options,
    }


@click.group(cls=HdxGroup)
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def transform(ctx: click.Context):
    """Apply pre-built public transforms to your tables."""
    user_profile: ProfileUserContext = ctx.parent.obj["usercontext"]
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


@click.command(cls=HdxCommand, name="list")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context):
    """List available integration {resource_plural}.

    \b
    Examples:
      # List all integration {resource_plural} available
      hdxcli integration transform list
    """
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


@click.command(cls=HdxCommand)
@click.argument("integration_transform_name")
@click.argument("transform_name")
@click.option(
    "--project",
    "project_name",
    required=True,
    help="The project to apply the transform to."
)
@click.option(
    "--table",
    "table_name",
    required=True,
    help="The table to apply the transform to."
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def apply(
    ctx: click.Context,
    integration_transform_name: str,
    transform_name: str,
    project_name: str,
    table_name: str,
):
    """Apply a public integration {resource} to your project.

    \b
    This command fetches a public transform by its INTEGRATION_TRANSFORM_NAME
    and creates it in your project with the new TRANSFORM_NAME.

    \b
    Examples:
      # Apply 'cloudtrail' and name it 'my-ct-transform' for 'my_proj.my_tbl'
      hdxcli integration transform apply cloudtrail my-cloudtrail-transform --project my_proj --table my_tbl
    """
    transform_contents = _basic_show(ctx, integration_transform_name)
    transform_dict = json.loads(transform_contents)
    try:
        del transform_dict["settings"]["is_default"]
    except KeyError:
        pass

    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, projectname=project_name, tablename=table_name)
    basic_transform(ctx)
    resource_path = ctx.obj["resource_path"]
    basic_create(user_profile, resource_path, transform_name, body=transform_dict)
    logger.info(f"Created transform {transform_name} from {integration_transform_name}")


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.option(
    "-i",
    "--indent",
    is_flag=True,
    default=False,
    help="Number of spaces for indentation in the output.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, resource_name: str, indent: bool):
    """Show the definition of a public integration {resource}.

    \b
    Examples:
      # Show the JSON definition for the 'cloudtrail' integration {resource}
      hdxcli integration transform show cloudtrail
    """
    logger.info(_basic_show(ctx, resource_name, indent=indent))


transform.add_command(list_)
transform.add_command(apply)
transform.add_command(show)
