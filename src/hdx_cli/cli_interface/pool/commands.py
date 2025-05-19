import click

from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from ...models import ProfileUserContext
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create

logger = get_logger()


@click.group(help="Pool-related operations")
@click.option(
    "--pool",
    "pool_name",
    help="Perform operation on the passed pool.",
    metavar="POOLNAME",
    default=None,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def pool(ctx: click.Context, pool_name: str):
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


@click.command(help="Create a new pool.")
@click.option(
    "--replicas",
    "-r",
    type=int,
    help="Number of replicas for the workload (default: 1)",
    required=False,
    default=1,
)
@click.option(
    "--cpu",
    "-c",
    type=float,
    help="Dedicated CPU allocation for each replica (default: 0.5)",
    required=False,
    default=0.5,
)
@click.option(
    "--memory",
    "-m",
    type=float,
    help="Dedicated memory allocation for each replica, expressed in Gi (default: 0.5)",
    required=False,
    default=0.5,
)
@click.option(
    "--storage",
    "-s",
    type=float,
    help="Storage capacity for each replica, expressed in Gi (default: 0.5)",
    required=False,
    default=0.5,
)
@click.argument("pool_service", metavar="POOLSERVICE")
@click.argument("pool_name", metavar="POOLNAME")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    replicas: int,
    cpu: float,
    memory: int,
    storage: int,
    pool_service: str,
    pool_name: str,
):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    pool_payload = _build_pool_payload(replicas, cpu, memory, storage, pool_service)
    basic_create(user_profile, resource_path, pool_name, body=pool_payload)
    logger.info(f"Created pool {pool_name}")


pool.add_command(command_list)
pool.add_command(create)
pool.add_command(command_delete)
pool.add_command(command_show)
pool.add_command(command_settings)
