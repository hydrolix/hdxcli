from functools import partial

import click

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import (
    basic_delete,
    basic_list,
    basic_show,
    basic_activity,
    basic_stats
)
from hdx_cli.library_api.common.exceptions import LogicException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import dynamic_confirmation_prompt, report_error_and_exit

logger = get_logger()


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(cls=HdxCommand, name="delete")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    help="Suppress confirmation to delete {resource}.",
    show_default=True,
    default=False,
)
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt: bool) -> None:
    """Delete a specific {resource}.

    This is a permanent action and cannot be undone. You will be prompted
    for confirmation unless `--disable-confirmation-prompt` is used.

    \b
    Examples:
      # Delete the {resource} named '{example_name}'
      {full_command_prefix} delete {example_name}
    """
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    if basic_delete(profile, resource_path, resource_name):
        logger.info(f"Deleted {resource_name}")
    else:
        logger.info(f"Could not delete {resource_name}. Not found")


@click.command(cls=HdxCommand, name="list")
@click.option("--page", "-p", type=int, default=1, help="Page number.")
@click.option("--page-size", "-s", type=int, default=None, help="Number of items per page.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_(ctx: click.Context, page: int, page_size: int) -> None:
    """List all available {resource_plural}.

    Retrieves a list of all {resource_plural} you have access to.
    Pagination options (`--page`, `--page-size`) are available if supported by the API.

    \b
    Examples:
      # List the first page of {resource_plural}
      {full_command_prefix} list
    """
    resource_path = ctx.parent.obj.get("resource_path")
    profile = ctx.parent.obj.get("usercontext")
    basic_list(profile, resource_path, page=page, page_size=page_size)


@click.command(cls=HdxCommand, name="show")
@click.argument("resource_name", required=False, default=None)
@click.option("-i", "--indent", is_flag=True, default=False, help="Indent the output.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def show(ctx: click.Context, resource_name: str, indent: bool) -> None:
    """Show details for a specific {resource}.

    Retrieves and displays the settings of a single {resource}.
    If no name is provided, the default {resource} will be used if exists.

    \b
    Examples:
      # Show details for the {resource} named '{example_name}'
      {full_command_prefix} show {example_name}
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    resource_kind = ctx.parent.command.name

    # Prioritize the argument from the command line
    effective_name = resource_name
    if not effective_name:
        effective_name = getattr(profile, resource_kind + "name", None)

    if not effective_name:
        raise LogicException(
            f"No default {resource_kind} found in profile and none provided as argument."
        )
    logger.info(basic_show(profile, resource_path, effective_name, indent=indent))


@click.command(cls=HdxCommand, name="activity")
@click.argument("resource_name", required=False, default=None)
@click.option("--page", "-p", type=int, default=1, help="Page number.")
@click.option("--page-size", "-s", type=int, default=None, help="Number of items per page.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def activity(ctx: click.Context, resource_name: str, page: int, page_size: int) -> None:
    """Shows the log of recent activities for the provided {resource}.

    \b
    Examples:
      # Show activity for the {resource} '{example_name}'
      {full_command_prefix} activity {example_name}
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    resource_kind = ctx.parent.command.name

    # Prioritize the argument from the command line
    effective_name = resource_name
    if not effective_name:
        effective_name = getattr(profile, resource_kind + "name", None)

    if not effective_name:
        raise LogicException(
            f"No default {resource_kind} found in profile and none provided as argument."
        )

    basic_activity(profile, resource_path, effective_name, page=page, page_size=page_size)


@click.command(cls=HdxCommand, name="stats")
@click.argument("resource_name", required=False, default=None)
@click.option("-i", "--indent", is_flag=True, default=False, help="Indent the output.")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def stats(ctx: click.Context, resource_name:str, indent: bool) -> None:
    """Shows usage and other statistics for the provided {resource}.

    \b
    Examples:
      # Show stats for the default {resource}
      {full_command_prefix} stats
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")
    resource_kind = ctx.parent.command.name

    # Prioritize the argument from the command line
    effective_name = resource_name
    if not effective_name:
        effective_name = getattr(profile, resource_kind + "name", None)

    if not effective_name:
        raise LogicException(
            f"No default {resource_kind} found in profile and none provided as argument."
        )

    basic_stats(profile, resource_path, effective_name, indent=indent)
