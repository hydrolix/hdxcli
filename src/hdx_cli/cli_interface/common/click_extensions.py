import click
import inflect
import re
import inspect

from hdx_cli.library_api.common.logging import get_logger

_inflect_engine = inflect.engine()
logger = get_logger()


def _get_depth(ctx: click.Context) -> int:
    """Calculates the nesting depth of a command context."""
    depth = 0
    current_ctx = ctx
    while current_ctx.parent:
        depth += 1
        current_ctx = current_ctx.parent
    return depth


_RESOURCE_CONTEXT_OPTIONS = {
    "table": ["project"],
    "function": ["project"],
    "dictionary": ["project"],
    "shadow": ["project"],
    "transform": ["project", "table"],
    "view": ["project", "table"],
    "column": ["project", "table"],
    "kinesis": ["project", "table"],
    "kafka": ["project", "table"],
    "siem": ["project", "table"],
    "stream": ["project", "table"],
}


def _get_full_command_prefix(ctx: click.Context) -> str:
    """Constructs the full command prefix including necessary parent context options."""
    if isinstance(ctx.command, click.Group):
        group_name = ctx.command.name
        base_path = ctx.command_path
    elif ctx.parent and isinstance(ctx.parent.command, click.Group):
        group_name = ctx.parent.command.name
        base_path = ctx.parent.command_path
    else:
        return ctx.command_path

    prefix_parts = [base_path]
    options_needed = _RESOURCE_CONTEXT_OPTIONS.get(group_name, [])

    for opt_name in options_needed:
        prefix_parts.append(f"--{opt_name} my_{opt_name.replace('-', '_')}")

    return " ".join(prefix_parts)


def _create_format_args(ctx: click.Context) -> dict:
    """Creates the dictionary of placeholders for formatting help text."""
    command = ctx.command
    resource = "resource"
    if isinstance(command, click.Group):
        resource = command.name or "resource"
    elif ctx.parent:
        resource = ctx.parent.command.name or "resource"

    singular_form = resource
    if singular_form == "files":
        singular_form = "file"

    return {
        "resource": resource.replace('-', ' '),
        "resource_plural": _inflect_engine.plural(singular_form).replace('-', ' '),
        "parent_command": ctx.parent.command_path if ctx.parent else "hdxcli",
        "example_name": f"my_{resource.replace('-', '_')}",
        "full_command_prefix": _get_full_command_prefix(ctx),
    }


def _parse_docstring_for_markdown(docstring: str, format_args: dict) -> (str, str, str):
    """Parses a docstring specifically for markdown generation."""
    if not docstring:
        return "", "", ""

    # Split markdown-only content
    cleaned_doc = inspect.cleandoc(docstring)
    parts = cleaned_doc.split('\n---\n', 1)
    doc_part = parts[0]
    markdown_only_part = parts[1] if len(parts) > 1 else ""

    # Format placeholders, removing paragraph breaks for markdown
    try:
        formatted_doc = doc_part.replace('\b', '').format(**format_args)
    except KeyError:
        formatted_doc = doc_part

    # Split description and examples
    example_parts = re.split(r'\n\s*Examples?:\s*\n', formatted_doc, 1, re.IGNORECASE)
    description = example_parts[0].strip()
    examples = example_parts[1].strip() if len(example_parts) > 1 else ""
    # Fix double newlines in examples caused by \n\n from \b\n
    examples = re.sub(r'\n\s*\n', '\n\n', examples)

    return description, examples, markdown_only_part


class HdxCommand(click.Command):
    """A custom click.Command."""

    def to_markdown(self, ctx: click.Context) -> str:
        """Generates Markdown documentation for a command."""
        # Update metavar before any text is generated
        resource = ctx.parent.command.name if ctx.parent else self.name
        for param in self.get_params(ctx):
            if isinstance(param, click.Argument) and param.name == "resource_name":
                param.metavar = f"{resource.replace('-', '_').upper()}_NAME"

        depth = _get_depth(ctx)
        heading = '#' * depth
        command_title = self.name.replace('-', ' ').replace('_', ' ').title()
        md_parts = [f"{heading} {command_title}\n"]

        format_args = _create_format_args(ctx)
        description, examples_str, markdown_only_content = _parse_docstring_for_markdown(self.help, format_args)

        if description:
            md_parts.append(f"{description}\n")

        usage_line = self.get_usage(ctx).replace('Usage: ', '')
        md_parts.append(f"**Usage**\n\n```bash\n{usage_line}\n```\n")

        opts = [p.get_help_record(ctx) for p in self.get_params(ctx) if
                p.get_help_record(ctx) and '--help' not in p.opts]
        if opts:
            md_parts.append("**Options**\n| Option | Description |\n|:-------|:------------|")
            for opt, desc in opts:
                try:
                    formatted_desc = desc.format(**format_args)
                except KeyError:
                    formatted_desc = desc
                sanitized_desc = formatted_desc.replace('|', '\\|').replace('\n', ' ')
                md_parts.append(f"| `{opt}` | {sanitized_desc} |")
            md_parts.append("")

        if examples_str:
            md_parts.append("**Examples**\n")
            cleaned_examples = inspect.cleandoc(examples_str)
            md_parts.append(f"```bash\n{cleaned_examples}\n```\n")

        if markdown_only_content:
            md_parts.append(f"\n{inspect.cleandoc(markdown_only_content)}\n")

        return "\n".join(md_parts)

    def get_help(self, ctx: click.Context) -> str:
        """Formats the command's help text with dynamic placeholders, ignoring markdown-only content."""
        # Update metavar before calling super().get_help()
        resource = ctx.parent.command.name if ctx.parent else self.name
        for param in self.params:
            if isinstance(param, click.Argument) and param.name == "resource_name":
                param.metavar = f"{resource.replace('-', '_').upper()}_NAME"

        # Temporarily use only the help-relevant part of the docstring
        original_help = self.help
        if original_help:
            self.help = inspect.cleandoc(original_help).split('\n---\n', 1)[0]

        # Click default, well-formatted help generation
        help_text = super().get_help(ctx)
        self.help = original_help

        if not help_text:
            return ""

        format_args = _create_format_args(ctx)
        try:
            return help_text.format(**format_args)
        except KeyError as e:
            logger.error(f"Error formatting help text for command '{self.name}': {e}.")
            return help_text


class HdxGroup(click.Group):
    """A custom click.Group."""

    def to_markdown(self, ctx: click.Context) -> str:
        depth = _get_depth(ctx)
        heading = '#' * depth
        group_title = self.name.replace('_', ' ').title()
        md_parts = [f"{heading} {group_title}\n"]

        format_args = _create_format_args(ctx)
        description, _, markdown_only_content = _parse_docstring_for_markdown(self.help, format_args)

        if description:
            md_parts.append(f"{description}\n")

        usage_line = self.get_usage(ctx).replace('Usage: ', '')
        md_parts.append(f"**Usage**\n\n```bash\n{usage_line}\n```\n")

        opts = [p.get_help_record(ctx) for p in self.get_params(ctx) if
                p.get_help_record(ctx) and '--help' not in p.opts]
        if opts:
            md_parts.append("**Options**\n| Option | Description |\n|:-------|:------------|")
            for opt, desc in opts:
                try:
                    formatted_desc = desc.format(**format_args)
                except KeyError:
                    formatted_desc = desc
                sanitized_desc = formatted_desc.replace('|', '\\|').replace('\n', ' ')
                md_parts.append(f"| `{opt}` | {sanitized_desc} |")
            md_parts.append("")

        if markdown_only_content:
            md_parts.append(f"\n{inspect.cleandoc(markdown_only_content)}\n")

        # Render subcommands
        direct_commands_md, subgroups_md = [], []
        for cmd_name in self.list_commands(ctx):
            cmd = self.get_command(ctx, cmd_name)
            if not cmd or cmd.hidden: continue
            sub_ctx = click.Context(cmd, info_name=cmd_name, parent=ctx)
            cmd_md = cmd.to_markdown(sub_ctx)
            (subgroups_md if isinstance(cmd, click.Group) else direct_commands_md).append(cmd_md)

        if direct_commands_md: md_parts.append("\n\n".join(direct_commands_md))
        if subgroups_md:
            if direct_commands_md: md_parts.append("\n\n")
            md_parts.append("\n\n".join(subgroups_md))

        return "\n".join(md_parts)

    def get_help(self, ctx: click.Context) -> str:
        """Formats the group's help text with dynamic placeholders."""
        original_help = self.help
        if original_help:
            self.help = inspect.cleandoc(original_help).split('\n---\n', 1)[0]

        help_text = super().get_help(ctx)
        self.help = original_help

        if not help_text:
            return ""

        format_args = _create_format_args(ctx)
        try:
            return help_text.format(**format_args)
        except KeyError as e:
            logger.debug(f"Error formatting help for group '{self.name}': {e}.")
            return help_text

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Writes all the sub-commands into the formatter, replacing placeholders."""
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            if cmd is None or cmd.hidden: continue

            short_help = cmd.get_short_help_str(limit=formatter.width)
            format_args = _create_format_args(click.Context(cmd, parent=ctx))
            try:
                formatted_short_help = short_help.format(**format_args)
            except KeyError:
                formatted_short_help = short_help

            commands.append((subcommand, formatted_short_help))

        if commands:
            with formatter.section("Commands"):
                formatter.write_dl(commands)

    def invoke(self, ctx: click.Context):
        is_help_for_subcommand = any(arg in ctx.help_option_names for arg in ctx.args)
        if is_help_for_subcommand:
            original_callback = self.callback
            self.callback = None
            try:
                return super().invoke(ctx)
            finally:
                self.callback = original_callback
        else:
            return super().invoke(ctx)
