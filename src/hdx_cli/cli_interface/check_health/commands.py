import click

from ...library_api.common.exceptions import ResourceNotFoundException
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.logging import get_logger

logger = get_logger()


@click.command(name="check-health",
               help="Checks the integrity of transforms and auto-views in a Hydrolix cluster. "
                    "If no arguments are provided, all projects and tables will be checked. "
                    "You can optionally specify a PROJECT_NAME to check only that project,"
                    "or both PROJECT_NAME and TABLE_NAME to narrow it down to a specific table.")
@click.argument("project_name", metavar="PROJECT_NAME", required=False, default=None, type=str)
@click.argument("table_name", metavar="TABLE_NAME", required=False, default=None, type=str)
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
    profile = ctx.parent.obj["usercontext"]
    if project_name and table_name:
        click.echo(f"Checking health of transforms in project '{project_name}' and table '{table_name}'...")
    elif project_name:
        click.echo(f"Checking health of transforms in project '{project_name}'...")
    else:
        click.echo("Checking health of transforms for the entire org...")

    _check_health(profile, project_name, table_name)


class ConflictReporter:
    """ Load with an auto-view and then run transforms through it to find issues """
    MAP_TYPES = ("array", "map")

    RESOLUTION_MAP = {
        "milliseconds": "ms",
        "millisecond": "ms",
        "millis": "ms",
        "milli": "ms",
        "ms": "ms",
        "seconds": "s",
        "second": "s",
        "secs": "s",
        "sec": "s",
        "s": "s",
    }

    COMPOSITE_INDEXABLE_TYPES = [
        "string",
        "bool",
        "boolean",
        "uint8",
        "int8",
        "uint16",
        "int16",
        "uint32",
        "int32",
        "uint64",
        "int64",
        "json",
    ]
    GLOBALLY_INDEXABLE_TYPES = COMPOSITE_INDEXABLE_TYPES + [
        "array",
        "map",
        "datetime",
        "datetime64",
        "epoch",
    ]

    def __init__(self, auto_view: dict) -> None:
        self.view_primary_columns = set()
        self.transform_primary_columns = set()
        self.used_columns = set()
        self.columns = self._build_auto_view_reference(auto_view)

    def _build_auto_view_reference(self, auto_view: dict) -> dict:
        column_reference = {}
        auto_view_settings = auto_view.get("settings", {}) or {}
        column_data = auto_view_settings.get("output_columns", [])
        for column in column_data:
            name = column.get("name")
            datatype = column.get("datatype", {})
            primary = datatype.get("primary", False)
            column_reference[name] = {
                "type": datatype.get("type"),
                "primary": primary,
                "resolution": datatype.get("resolution", None),
                "elements": datatype.get("elements", []),
                "index": datatype.get("index", None),
            }
            if primary:
                self.view_primary_columns.add(name)
        return column_reference

    @staticmethod
    def get_view_datatype(type: str, resolution: str = None) -> str:
        """ Normalize type on transform column to match view types """
        if type in ("bool", "boolean"):
            return "uint8"
        if type in ("datetime", "epoch"):
            if resolution == "ms":
                return "datetime64"
            else:
                return "datetime"
        return type
    
    def _check_elements_index(self, column_name: str, elements: list) -> list[str]:
        messages = []
        for element in elements:
            type = element.get("type")
            index = element.get("index")
            indexable = type in self.COMPOSITE_INDEXABLE_TYPES
            if index and not indexable:
                messages.append(f"[ERROR] '{column_name}' element {type} not indexable")
            if type in ["array", "map"]:
                sub_elements = element.get("elements", [])
                messages += self._check_elements_index(column_name, sub_elements)
        return messages

    def _check_transform_column(self, transform_column: dict) -> list[str]:
        """ Check a single column for all problems """
        messages = []
        transform_column_name = transform_column.get("name")
        transform_column_datatype = transform_column.get("datatype", {})
        view_column = self.columns.get(transform_column_name, {})

        # Suppressed/ignored transform columns should not appear in auto-view
        suppress = transform_column_datatype.get("suppress", False)
        ignore = transform_column_datatype.get("ignore", False)
        if suppress or ignore:
            if view_column:
                messages.append(f"[WARN] '{transform_column_name}' - in auto-view while suppressed/ignored")
            return messages

        # Track which columns have appeared in transforms to see if any extraneous ones are in the view
        self.used_columns.add(transform_column_name)

        # A non-ignored column appears in a transform, but not the auto-view
        if not view_column:
            messages.append(f"[ERROR] '{transform_column_name}' - not present in auto-view")
            return messages

        # A transform column type does not match view column type
        transform_column_type = transform_column_datatype.get("type")
        transform_column_resolution = self.RESOLUTION_MAP.get(
            transform_column_datatype.get("resolution", "seconds")
        )
        transform_column_view_type = self.get_view_datatype(
            transform_column_type,
            resolution=transform_column_resolution,
        )

        view_column_type = view_column.get("type")
        if view_column_type != transform_column_view_type:
            messages.append(
                f"[CONFLICT] '{transform_column_name}' - transform_type: {transform_column_view_type}, view_type: {view_column_type}"
            )

        # transform column index does not match view column index
        view_column_index = view_column.get("index", None)
        transform_column_index = transform_column_datatype.get("index", None)
        if view_column_index != transform_column_index:
            messages.append(
                f"[CONFLICT] '{transform_column_name}' - transform_index: {transform_column_index}, view_index: {view_column_index}"
            )

        # If index true, verify column is indexable
        if transform_column_index:
            if transform_column_type not in self.GLOBALLY_INDEXABLE_TYPES:
                messages.append(
                    f"[ERROR] '{transform_column_name}' - Index is true but {transform_column_type} columns are not indexable"
                )

        # transform column resolution does not match view column resolution
        view_column_resolution = self.RESOLUTION_MAP.get(
            view_column.get("resolution", "seconds")
        )
        if view_column_resolution != transform_column_resolution:
            messages.append(
                f"[CONFLICT] '{transform_column_name}' - transform_resolution: {transform_column_resolution}, view_resolution: {view_column_resolution}"
            )
        
        # transform column primary does not match view column primary
        transform_column_primary = transform_column_datatype.get("primary", False)
        view_column_primary = view_column.get("primary", False)
        if transform_column_primary != view_column_primary:
            messages.append(
                f"[CONFLICT] '{transform_column_name}' - transform_primary: {transform_column_primary}, view_primary: {view_column_primary}"
            )
        
        # If field is primary, track it for later
        if transform_column_primary:
            self.transform_primary_columns.add(transform_column_name)

        # array/map column must have elements matching between transform and view
        transform_column_is_map = transform_column_view_type in self.MAP_TYPES
        view_column_is_map = view_column_type in self.MAP_TYPES
        if transform_column_is_map or view_column_is_map:
            transform_column_elements = transform_column_datatype.get("elements", [])
            view_column_elements = view_column.get("elements", [])
            normalized_transform_elements = self.normalize_elements(transform_column_elements)
            if normalized_transform_elements != view_column_elements:

                messages.append(
                    f"[CONFLICT] '{transform_column_name}' - transform_elements: {transform_column_elements}, view_elements: {view_column_elements}",
                )
            messages += self._check_elements_index(transform_column_name, normalized_transform_elements)
        return messages

    def normalize_elements(self, elements: list) -> list:
        """ recursively normalize the types of elements """
        normalized_elements = []
        for element in elements:
            normalized_type = self.get_view_datatype(
                element.get("type", None),
                element.get("resolution", None)
            )
            element["type"] = normalized_type
            sub_elements = element.get("elements", [])
            if sub_elements:
                element["elements"] = self.normalize_elements(sub_elements)
            normalized_elements.append(element)
        return normalized_elements

    def transform_report(self, transform: dict) -> dict:
        """ Check all columns for problems """
        report = {}
        columns = transform.get("settings", {}).get("output_columns", [])
        for column in columns:
            transform_check = self._check_transform_column(column)
            if transform_check:
                column_name = column.get("name")
                report[column_name] = transform_check
        return report

    def auto_view_report(self) -> dict:
        report = {}
        for column_name, datatype in self.columns.items():
            messages = []
            column_type = datatype.get('type', None)
            column_index = datatype.get('index', None)
            if column_index:
                if column_type not in self.GLOBALLY_INDEXABLE_TYPES:
                    messages.append(
                        f"[ERROR] 'Index is true on auto_view, but {column_type} columns are not indexable"
                    )
            if column_type in ["array", "map"]:
                column_elements = datatype.get("elements", [])
                messages += self._check_elements_index(column_name, column_elements)
            if messages:
                report[column_name] = messages
        return report

    def unused_columns(self) -> set[str]:
        """ Which view columns have not appeared in a checked transform yet? """
        view_columns = set(self.columns.keys())
        return view_columns - self.used_columns
    
    def primary_column_report(self):
        messages = []
        view_primary_column_count = len(self.view_primary_columns)
        if view_primary_column_count != 1:
            messages.append(f"[ERROR] auto_view has {view_primary_column_count} primary columns and it should have exactly 1: {self.view_primary_columns}")
        transform_primary_column_count = len(self.transform_primary_columns)
        if transform_primary_column_count != 1:
            messages.append(f"[ERROR] transform has {transform_primary_column_count} primary columns and it should have exactly 1: {self.transform_primary_columns}")
        if self.view_primary_columns != self.transform_primary_columns:
            messages.append(f"[CONFLICT] transform primary columns {self.transform_primary_columns} must match auto_view primary column {self.view_primary_columns}")
        return messages

def _check_health(profile: ProfileUserContext, target_project_name: str, target_table_name: str):
    """ Check the integrity of transforms and auto-views in a Hydrolix cluster """
    projects, _ = access_resource_detailed(profile, [("projects", target_project_name)])
    # If 'target_project_name' is not provided, projects will contain a list of all projects in the org
    # If 'target_project_name' is provided, projects will contain a simple dict with the project details
    projects = [projects] if isinstance(projects, dict) else projects

    for project in projects:
        project_name = project.get("name")

        tables, _ = access_resource_detailed(
            profile, [("projects", project_name), ("tables", target_table_name)]
        )
        # If 'target_table_name' is not provided, tables will contain a list of all tables in the project
        # If 'target_table_name' is provided, tables will contain a simple dict with the table details
        tables = [tables] if isinstance(tables, dict) else tables

        for table in tables:
            table_name = table.get("name")

            transforms, _ = access_resource_detailed(
                profile, [
                    ("projects", project_name),
                    ("tables", table_name),
                    ("transforms", None)]
            )
            try:
                auto_view, _ = access_resource_detailed(
                    profile, [
                        ("projects", project_name),
                        ("tables", table_name),
                        ("views", "auto_view")]
                )
            except ResourceNotFoundException:
                logger.debug(f"Auto-view not found for {project_name}.{table_name}")
                auto_view = None

            # One reporter per-table loaded with the auto-view
            if not auto_view or not transforms:
                logger.info(f"# {project_name}.{table_name}")
                if transforms:
                        logger.info(f"- [ERROR] Table has transforms, but no auto-view")
                elif auto_view:
                        logger.info(f"- [ERROR] Table has auto-view, but no transforms")
                else:
                    logger.info(f"- [WARN] Empty table, no transforms or auto-view")
                logger.info("")
                continue

            reporter = ConflictReporter(auto_view)
            reports = {}

            reports[f"auto_view"] = reporter.auto_view_report()

            for transform in transforms:
                transform_name = transform.get("name")
                reports[transform_name] = reporter.transform_report(transform) or {}
            # Print unused columns report for table
            unused_columns = reporter.unused_columns()
            if unused_columns:
                logger.info(f"# {project_name}.{table_name}")
                logger.info("Columns in auto-view, but not any transform")
                for unused_column in unused_columns:
                    logger.info(f"- {unused_column}")
                logger.info("")

            # print report for each transform on the table
            for transform_name, transform_report in reports.items():
                logger.info(f"# {project_name}.{table_name}.{transform_name}")
                for report_column, report_messages in transform_report.items():
                    if report_messages:
                        for message in report_messages:
                            logger.info(f"- {message}")
                if not transform_report:
                    if transform_name == "auto_view":
                        logger.info("- [OK] auto_view is good")
                    else:
                        logger.info("- [OK] Transform is good")
                logger.info("")

            # print report if primary columns are incorrect
            for message in reporter.primary_column_report():
                logger.info(message)
