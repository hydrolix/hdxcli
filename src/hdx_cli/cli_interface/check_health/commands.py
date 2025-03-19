import click

from ...library_api.common.context import ProfileUserContext
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from . import table_cleaner, utils

logger = get_logger()


@click.command(
    name="check-health",
    help="Checks the integrity of transforms and auto-views in a Hydrolix cluster. "
    "If no arguments are provided, all projects and tables will be checked. "
    "You can optionally specify a PROJECT_NAME to check only that project,"
    "or both PROJECT_NAME and TABLE_NAME to narrow it down to a specific table.",
)
@click.argument(
    "project_name", metavar="PROJECT_NAME", required=False, default=None, type=str
)
@click.argument(
    "table_name", metavar="TABLE_NAME", required=False, default=None, type=str
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def check_health(ctx: click.Context, project_name: str, table_name: str):
    """
    This command checks the integrity of the transforms and auto-views.

    - If no arguments are provided, it checks the entire org.
    - If a project_name is provided, it checks transforms within that project.
    - If both project_name and table_name are provided, it checks only transforms within that table.
    """
    parent = getattr(ctx, "parent")
    if parent is None:
        return
    profile = parent.obj["usercontext"]
    if project_name and table_name:
        click.echo(
            f"Checking health of transforms in project '{project_name}' and table '{table_name}'..."
        )
    elif project_name:
        click.echo(f"Checking health of transforms in project '{project_name}'...")
    else:
        click.echo("Checking health of transforms for the entire org...")

    _check_health(profile, project_name, table_name)


def _check_health(
    profile: ProfileUserContext, target_project_name: str, target_table_name: str
):
    """Check the integrity of transforms and auto-views in a Hydrolix cluster"""
    projects, _ = access_resource_detailed(profile, [("projects", target_project_name)])
    # If 'target_project_name' is not provided, projects will contain a list of all projects in the org
    # If 'target_project_name' is provided, projects will contain a simple dict with the project details
    projects = [projects] if isinstance(projects, dict) else projects
    if not projects:
        logger.info("[INFO] Cluster is healthy — There are no projects")
        return

    for project in projects:
        project_name = project.get("name", "")
        utils.print_header(project_name, underline_char="+")

        tables, _ = access_resource_detailed(
            profile, [("projects", project_name), ("tables", target_table_name)]
        )
        # If 'target_table_name' is not provided, tables will contain a list of all tables in the project
        # If 'target_table_name' is provided, tables will contain a simple dict with the table details
        tables = [tables] if isinstance(tables, dict) else tables
        if not tables:
            logger.info(f"\n[INFO] Project '{project_name}' has no tables")
            continue

        for table in tables:
            table_name = table.get("name")
            views, _ = access_resource_detailed(
                profile,
                [
                    ("projects", project_name),
                    ("tables", table_name),
                    ("views", None),
                ],
            )

            transforms, _ = access_resource_detailed(
                profile,
                [
                    ("projects", project_name),
                    ("tables", table_name),
                    ("transforms", None),
                ],
            )
            if (not transforms) and (not views):
                logger.info(
                    f"\n[INFO] Table '{project_name}.{table_name}' skipped — no views or transforms"
                )
                continue

            cleaner = table_cleaner.TableCleaner(
                table=table, transforms=transforms, views=views
            )
            cleaner.table_report()
