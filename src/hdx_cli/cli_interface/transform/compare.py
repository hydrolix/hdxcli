import copy
from pathlib import Path

import click

from hdx_cli.auth.context_builder import load_user_context
from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_transform
from hdx_cli.cli_interface.transform.comparator.engine import TransformComparator
from hdx_cli.config.paths import PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext, ProfileLoadContext

logger = get_logger()


def _fetch_transform_from_cluster(
    ctx: click.Context,
    project_name: str,
    table_name: str,
    transform_name: str
) -> dict:
    """Fetches a single transform's configuration from
    the cluster. Returns the configuration as a dictionary."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, transformname=transform_name
    )
    basic_transform(ctx)
    return ctx.obj.get("specific_resource")


def _load_transform_spec(
    ctx: click.Context,
    specifier: str,
) -> tuple[dict, str]:
    """Loads a transform from a local file path or a cluster reference.
    A cluster reference is in the format 'project.table.transform'.
    Returns a tuple of (transform_data, description)."""
    if Path(specifier).is_file() and specifier.endswith('.json'):
        logger.debug(f"Loading transform from local file: {specifier}")
        return read_json_from_file(specifier), f"local file: {specifier}"

    # Check for cluster reference format: project.table.transform
    parts = specifier.split('.')
    if len(parts) == 3:
        project, table, transform = parts
        user_profile = ctx.parent.obj["usercontext"]
        logger.debug(f"Fetching transform '{transform}' from cluster '{user_profile.profilename}'...")
        data = _fetch_transform_from_cluster(ctx, project, table, transform)
        return data, f"cluster '{user_profile.profilename}': {specifier}"

    raise click.BadParameter(
        f"Invalid format for '{specifier}'. Provide a path to a .json file or a "
        "cluster reference like 'project_name.table_name.transform_name'."
    )


@click.command(cls=HdxCommand, name="compare")
@click.argument("transform_a_spec", metavar="TRANSFORM_A")
@click.argument("transform_b_spec", metavar="TRANSFORM_B")
@click.option(
    "--profile-b",
    help="Profile for fetching TRANSFORM_B, if different from the default.",
    metavar="PROFILE_NAME",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def compare(
    ctx_a: click.Context,
    transform_a_spec: str,
    transform_b_spec: str,
    profile_b: str
):
    """Compares two transforms, showing differences in their settings.

    Transforms can be specified as a local JSON file path or as a reference
    to a transform on a cluster in the format: `project_name.table_name.transform_name`.

    When comparing a transform on a different cluster, use `--profile-b` to
    specify the profile details for `TRANSFORM_B`.

    \b
    Examples:
      # Compare two local JSON files
      hdxcli transform compare path/to/transform_A.json path/to/transform_B.json

    \b
      # Compare a local file to a transform on the default profile
      hdxcli transform compare path/to/transform_A.json my_project.my_table.my_transform

    \b
      # Compare two transforms on the same 'default' profile
      hdxcli transform compare proj_A.tbl_A.tf_A proj_B.tbl_B.tf_B

    \b
      # Compare a transform on the 'dev' profile to one on another (profile 'prod')
      hdxcli --profile dev transform compare p1.t1.tf1 p2.t2.tf2 --profile-b prod
    """
    ctx_b = copy.deepcopy(ctx_a)
    # Determine ctx for TRANSFORM_B
    if profile_b:
        load_context = ProfileLoadContext(name=profile_b, config_file=PROFILE_CONFIG_FILE)
        profile_for_b = load_user_context(load_context)
        ctx_b.parent.obj["'profilecontext'"] = load_context
        ctx_b.parent.obj["usercontext"] = profile_for_b

    # Load both transforms
    data_a, desc_a = _load_transform_spec(ctx_a, transform_a_spec)
    data_b, desc_b = _load_transform_spec(ctx_b, transform_b_spec)

    comparator = TransformComparator(data_a, data_b, desc_a, desc_b)
    comparator.run()
    comparator.display()
