from functools import partial

import click

from ...library_api.common.exceptions import LogicException
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import dynamic_confirmation_prompt, report_error_and_exit
from ...library_api.utility.functions import heuristically_get_resource_kind
from .undecorated_click_commands import (
    basic_activity,
    basic_delete,
    basic_list,
    basic_show,
    basic_stats,
)

logger = get_logger()


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(help="Delete resource.")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    help="Suppress confirmation to delete resource.",
    show_default=True,
    default=False,
)
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    if basic_delete(profile, resource_path, resource_name):
        logger.info(f"Deleted {resource_name}")
    else:
        logger.info(f"Could not delete {resource_name}. Not found")


@click.command(help="List resources.", name="list")
@click.option("--page", "-p", type=int, default=1, help="Page number.")
@click.option("--page-size", "-s", type=int, default=None, help="Number of items per page.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, page: int, page_size: int):
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    basic_list(profile, resource_path, page=page, page_size=page_size)


@click.command(
    help="Show resource. If not resource_name is provided, it will show the default "
    "if there is one."
)
@click.argument("resource_name", required=False, default=None)
@click.option("-i", "--indent", is_flag=True, default=False, help="Indent the output.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, resource_name: str, indent: bool):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _, resource_kind = heuristically_get_resource_kind(resource_path)

    # Prioritize the argument from the command line
    effective_name = resource_name
    if not effective_name:
        effective_name = getattr(profile, resource_kind + "name", None)

    if not effective_name:
        raise LogicException(
            f"No default {resource_kind} found in profile and none provided as argument."
        )
    logger.info(basic_show(profile, resource_path, effective_name, indent=indent))


@click.command(
    help="Display the activity of a resource. If not resource_name is provided, "
    "it will show the default if there is one."
)
@click.option("--page", "-p", type=int, default=1, help="Page number.")
@click.option("--page-size", "-s", type=int, default=None, help="Number of items per page.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def activity(ctx: click.Context, page: int, page_size: int):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + "name")):
        raise LogicException(f"No default {resource_kind} found in profile")

    basic_activity(profile, resource_path, resource_name, page=page, page_size=page_size)


@click.command(
    help="Display statistics for a resource. If not resource_name is provided, "
    "it will show the default if there is one."
)
@click.option("-i", "--indent", is_flag=True, default=False, help="Indent the output.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def stats(ctx: click.Context, indent: bool):
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    _, resource_kind = heuristically_get_resource_kind(resource_path)
    if not (resource_name := getattr(profile, resource_kind + "name")):
        raise LogicException(f"No default {resource_kind} found in profile")

    basic_stats(profile, resource_path, resource_name, indent=indent)
