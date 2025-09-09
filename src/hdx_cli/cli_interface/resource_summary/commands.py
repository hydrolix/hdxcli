from typing import Optional, Union

import click
from rich.console import Console
from rich.rule import Rule
from rich.table import Table

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_get
from hdx_cli.library_api.common.exceptions import ConfigurationNotFoundException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


def get_resource_count(profile: ProfileUserContext, path: str) -> int:
    response_data: Optional[Union[dict, list]] = basic_get(profile, path)

    if not response_data:
        return 0

    if isinstance(response_data, dict):
        return response_data.get('count', 0)

    if isinstance(response_data, list):
        return len(response_data)

    logger.debug(f"Unexpected data type received from '{path}'. Could not determine count.")
    return 0


@click.command(cls=HdxCommand, name="resource-summary")
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def resource_summary(ctx: click.Context):
    """Summarize the count of all resources in the organization.
    This command provides a quick overview of the total number of projects,
    tables, transforms, views, and other key resources that the current
    user has permission to view.

    \b
    Examples:
      # Display a summary of all resources
      {full_command_prefix} resource-summary
    """
    profile = ctx.parent.obj["usercontext"]
    resource_map = _resource_summary(profile)
    if not resource_map:
        return

    console.print(Rule("Resource Summary", style="dim", characters="─"))
    summary_table = Table(show_header=False, box=None, padding=(0, 0), pad_edge=False)
    summary_table.add_column(style="dim", no_wrap=True)
    summary_table.add_column()

    for resource_name, count in resource_map.items():
        summary_table.add_row(f"{resource_name}:", str(count))
    console.print(summary_table)


def _resource_summary(profile: ProfileUserContext) -> Optional[dict]:
    config_path = f"/config/v1/orgs/{profile.org_id}/config_blob/"
    resource_config = basic_get(profile, config_path)

    if not resource_config:
        raise ConfigurationNotFoundException(
            "Could not retrieve the main resource configuration blob."
        )

    # Counts from the main configuration blob
    final_counts = _count_resources_from_data(resource_config)

    # Counts from specific
    final_counts["Users & SA"] = get_resource_count(profile, "/config/v1/users/")
    final_counts["Roles"] = get_resource_count(profile, "/config/v1/roles/")

    alter_jobs_count = get_resource_count(
        profile, f"/config/v1/orgs/{profile.org_id}/jobs/alter/"
    )
    batch_jobs_count = get_resource_count(
        profile, f"/config/v1/orgs/{profile.org_id}/jobs/batch/"
    )
    final_counts["Jobs"] = alter_jobs_count + batch_jobs_count

    return final_counts


def _count_resources_from_data(data: dict) -> dict:
    """
    Count resources from the provided data structure. If the data is empty or None,
    it returns a dictionary with all counts set to zero.
    """
    counts = {
        "Projects": 0,
        "Tables": 0,
        "Transforms": 0,
        "Views": 0,
        "Functions": 0,
        "Dictionaries": 0,
        "Sources": 0,
        "Storages": 0,
        "Credentials": 0,
    }

    # Count Projects
    projects_data = data.get("projects", {})
    counts["Projects"] = len(projects_data)

    # Iterate through projects to count nested resources
    for project_details in projects_data.values():
        if not isinstance(project_details, dict):
            continue

        counts["Functions"] += len(project_details.get("functions", {}))
        counts["Dictionaries"] += len(project_details.get("dictionaries", {}))

        tables_data = project_details.get("tables", {})
        counts["Tables"] += len(tables_data)

        for table_details in tables_data.values():
            if not isinstance(table_details, dict):
                continue

            # Counting transforms and views
            counts["Transforms"] += len(table_details.get("transforms", {}))
            counts["Views"] += len(table_details.get("views", {}))

    # Count top-level Sources
    sources = data.get("sources", {})
    kafka_sources = len(sources.get("kafka", {}))
    siem_sources = len(sources.get("siem", {}))
    kinesis_sources = len(sources.get("kinesis", {}))
    summary_sources = len(sources.get("summary", {}))
    counts["Sources"] = kafka_sources + siem_sources + kinesis_sources + summary_sources

    # Count Storages (it's a list)
    counts["Storages"] = len(data.get("storages", []))

    # Count Credentials (it's a list)
    counts["Credentials"] = len(data.get("credentials", []))

    return counts
