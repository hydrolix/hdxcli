from typing import Any, Iterable, List, Tuple, Union

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_get
from hdx_cli.library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from hdx_cli.models import ProfileUserContext

console = Console()


def _find_max_key_width(data: Any, indent_level: int = 0) -> int:
    """
    Recursively finds the maximum display length of any key in the nested data.
    This is used to calculate the padding needed for perfect vertical alignment.
    """
    max_width = 0
    indent_space = "  " * indent_level

    if isinstance(data, dict):
        for key, value in data.items():
            current_width = len(f"{indent_space}{key}")
            max_width = max(max_width, current_width)

            if isinstance(value, (dict, list)) and value:
                max_width = max(max_width, _find_max_key_width(value, indent_level + 1))

    elif isinstance(data, list):
        for index, item in enumerate(data):
            current_width = len(f"{indent_space}[{index}]")
            max_width = max(max_width, current_width)
            if isinstance(item, (dict, list)) and item:
                max_width = max(max_width, _find_max_key_width(item, indent_level + 1))

    return max_width


def _add_rows_recursively(table: Table, data: Any, max_key_width: int, indent_level: int = 0):
    """
    Recursively populates a single-column table with perfectly aligned key-value pairs.
    This version is refactored to avoid code duplication.
    """
    indent_space = "  " * indent_level
    items: Iterable[Tuple[Union[str, int], Any]]

    # Determine the items to iterate over based on the data type.
    if isinstance(data, dict):
        items = data.items()

        def format_key(key_):
            return f"{indent_space}{key_}"

    elif isinstance(data, list):
        items = enumerate(data)

        def format_key(index):
            return f"{indent_space}[{index}]"

    else:
        # Should not be reached with proper recursion
        return

    for key, value in items:
        key_str = format_key(key)
        if isinstance(value, (dict, list)) and value:
            table.add_row(Text(key_str, style="bold"))
            _add_rows_recursively(table, value, max_key_width, indent_level + 1)
        else:
            padding = " " * (max_key_width - len(key_str) + 2)
            row_text = Text.assemble((key_str, "cyan"), padding, (str(value), "white"))
            table.add_row(row_text)


def _show_defaults(profile: ProfileUserContext, categories: List[str]):
    """Fetches default settings and displays them in structured panels."""
    defaults_data = basic_get(profile, "/config/v1/defaults/")
    if not defaults_data:
        console.print("No default settings found or the endpoint is unavailable.")
        return

    data_to_display = defaults_data
    if categories:
        data_to_display = {key: defaults_data[key] for key in categories if key in defaults_data}
        if not data_to_display:
            console.print(
                f"Category '{', '.join(categories)}' not found in defaults.",
                highlight=False,
            )
            return

    # Measure the widest key in the entire dataset for consistent alignment across all panels.
    max_width = _find_max_key_width(data_to_display)

    for category, data in data_to_display.items():
        title = f"[bold]{category.replace('_', ' ').title()} Defaults[/bold]"

        layout_table = Table(box=None, show_header=False, pad_edge=False)
        layout_table.add_column(overflow="fold")

        # Render the data, passing the calculated max_width for padding.
        _add_rows_recursively(layout_table, data, max_width)

        if layout_table.row_count > 0:
            panel = Panel(layout_table, title=title, border_style="dim", expand=False)
            console.print(panel)


@click.command(cls=HdxCommand, name="show-defaults")
@click.argument("category", nargs=-1, required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def show_defaults(ctx: click.Context, category: List[str]):
    """Show default settings for various resources.
    This command retrieves the default configurations from the API for resources
    such as tables, transforms, sources, and jobs. These defaults are applied
    when a new resource is created without specifying all its settings.

    You can view all defaults at once or filter the output by providing one or
    more category names (e.g., project, table, transforms) as arguments.

    \b
    Examples:
      # Display all default settings, grouped by category
      hdxcli show-defaults

    \b
      # Display only the defaults for tables and transforms
      hdxcli show-defaults table transforms
    """
    profile = ctx.parent.obj["usercontext"]
    _show_defaults(profile, category)
