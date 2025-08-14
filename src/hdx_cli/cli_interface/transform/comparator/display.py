import hashlib
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text


class ComparisonDisplay:
    """Renders the results of a transform comparison."""

    def __init__(self, transform_a, transform_b, results):
        self.transform_a = transform_a
        self.transform_b = transform_b
        self.results = results
        self.console = Console(theme=None)

    def render(self):
        """Renders the entire comparison report."""
        self._render_summary()
        self.console.print()
        self._render_properties()
        self.console.print()
        self._render_settings()
        self.console.print()
        self._render_columns()

    def _render_summary(self):
        summary_table = Table.grid(expand=False, padding=(0, 1))
        summary_table.add_column(style="cyan", no_wrap=True)
        summary_table.add_column()
        summary_table.add_row(
            "Transform A:",
            f"[bold]{self.transform_a.name}[/bold] ({self.transform_a.description})"
        )
        summary_table.add_row(
            "Transform B:",
            f"[bold]{self.transform_b.name}[/bold] ({self.transform_b.description})"
        )
        self.console.print(
            Panel(
                summary_table,
                title="[bold]Comparison Summary[/bold]",
                border_style="dim",
                expand=False
            )
        )

    def _render_properties(self):
        table = Table(
            title="[bold]Top-Level Properties[/bold]",
            show_header=True,
            header_style="bold default"
        )
        table.add_column("Property", style="cyan", width=15)
        table.add_column("Transform A", style="default")
        table.add_column("Transform B", style="default")
        table.add_column("Status", width=10)

        for item in self.results.get('properties', []):
            val_a, val_b = item['val_a'], item['val_b']
            are_equal = val_a == val_b
            status = "[bold green]✓ Match[/bold green]" if are_equal else "[bold red]✗ Differs[/bold red]"

            val_a_display = str(val_a) if val_a is not None else "[dim]Not Set[/dim]"
            val_b_display = str(val_b) if val_b is not None else "[dim]Not Set[/dim]"

            if item['key'] == 'sql':
                if are_equal:
                    val_a_display = "[dim]Identical SQL[/dim]"
                    val_b_display = "[dim]-[/dim]"
                else:
                    hash_a = hashlib.sha256(val_a.encode()).hexdigest() if val_a else "N/A"
                    hash_b = hashlib.sha256(val_b.encode()).hexdigest() if val_b else "N/A"
                    val_a_display = f"SHA256: [yellow]{hash_a[:12]}[/yellow]..."
                    val_b_display = f"SHA256: [yellow]{hash_b[:12]}[/yellow]..."

            table.add_row(item['key'].capitalize(), val_a_display, val_b_display, status)

        self.console.print(table)

    def _render_settings(self):
        diffs = self.results.get('settings', {}).get('diffs', [])

        if not diffs:
            self.console.print(Text("Other Settings: No differences found.", style="green"))
            return

        table = Table(
            title="[bold]Other Settings Differences[/bold]",
            show_header=True,
            header_style="bold default"
        )
        table.add_column("Setting Key", style="cyan")
        table.add_column("Transform A", style="default", max_width=50)
        table.add_column("Transform B", style="default", max_width=50)

        for diff in diffs:
            table.add_row(diff['key'], diff['val_a'], diff['val_b'])

        self.console.print(table)

    def _render_columns(self):
        data = self.results.get('output_columns', {})

        if not any(data.values()):
            self.console.print(Text("Output Columns: No differences found.", style="green"))
            return

        panel_content = Text()
        if only_in_a := data.get('only_in_a'):
            panel_content.append("Columns only in A:\n", style="bold red")
            content = "\n".join(f" - {item}" for item in only_in_a)
            panel_content.append(content)

        if only_in_b := data.get('only_in_b'):
            if panel_content:
                # The separator between the two sections
                panel_content.append("\n\n")

            panel_content.append("Columns only in B:\n", style="bold green")
            content = "\n".join(f" - {item}" for item in only_in_b)
            panel_content.append(content)

        if panel_content:
            self.console.print(
                Panel(
                    panel_content,
                    title="[bold]Output Columns - Additions/Deletions[/bold]",
                    border_style="dim",
                    expand=False
                )
            )

        if modified := data.get('modified'):
            self.console.print(Text("Modified Columns Details", style="bold default"))
            for mod in modified:
                diff_table = Table(show_header=True, header_style="bold dim", box=None)
                diff_table.add_column("Attribute", style="cyan")
                diff_table.add_column("Transform A", style="default")
                diff_table.add_column("Transform B", style="default")

                for diff in mod['diffs']:
                    diff_table.add_row(
                        diff['attribute'],
                        diff['value_a'],
                        diff['value_b']
                    )
                self.console.print(
                    Panel(
                        diff_table,
                        title=f"Column: [cyan]{mod['name']}[/cyan]",
                        border_style="dim",
                        expand=False
                    )
                )
