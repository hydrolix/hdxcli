import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@click.group(cls=HdxGroup)
@click.option(
    "--pool",
    "pool_name",
    default=None,
    help="Use or override pool set in the profile.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def pool(ctx: click.Context, pool_name: str):
    """Commands to create, list, and manage resource pools for
    cluster services."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, poolname=pool_name)
    ctx.obj = {"resource_path": f"/config/v1/pools/", "usercontext": user_profile}


def _build_pool_payload(
    replicas: int,
    cpu: float,
    memory: int,
    storage: int,
    pool_service: str,
) -> dict:
    """Build the payload for creating a new pool"""
    return {
        "description": "Created with hdxcli tool",
        "settings": {
            "k8s_deployment": {
                "replicas": str(replicas),
                "cpu": str(cpu),
                "memory": f"{memory}Gi",
                "storage": f"{storage}Gi",
                "service": pool_service,
            }
        },
    }


@click.command(cls=HdxCommand)
@click.argument("pool_name")
@click.argument("pool_service")
@click.option(
    "--replicas",
    "-r",
    type=int,
    help="Number of replicas for the workload (default: 1).",
    default=1,
)
@click.option(
    "--cpu",
    "-c",
    type=float,
    help="Dedicated CPU allocation for each replica (default: 0.5).",
    default=0.5,
)
@click.option(
    "--memory",
    "-m",
    type=float,
    help="Dedicated memory allocation for each replica, in Gi (default: 0.5).",
    default=0.5,
)
@click.option(
    "--storage",
    "-s",
    type=float,
    help="Storage capacity for each replica, in Gi (default: 0.5).",
    default=0.5,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    pool_name: str,
    pool_service: str,
    replicas: int,
    cpu: float,
    memory: int,
    storage: int,
):
    """Allocates resources (CPU, memory, storage) to create a
    new service {resource}.

    \b
    Examples:
      # Create a {resource} named '{example_name}' for the 'query-peer' service
      {full_command_prefix} create {example_name} query-peer --replicas 2 --cpu 1 --memory 2 --storage 10
    """
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    pool_payload = _build_pool_payload(replicas, cpu, memory, storage, pool_service)
    basic_create(user_profile, resource_path, pool_name, body=pool_payload)
    logger.info(f"Created {ctx.parent.command.name} {pool_name}")


pool.add_command(command_list)
pool.add_command(create)
pool.add_command(command_delete)
pool.add_command(command_show)
pool.add_command(command_settings)
