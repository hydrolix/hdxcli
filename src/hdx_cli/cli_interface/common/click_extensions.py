import inspect
import re

import click
import inflect

from hdx_cli.library_api.common.logging import get_logger

_inflect_engine = inflect.engine()
logger = get_logger()


def _get_depth(ctx: click.Context) -> int:
    """Calculates the nesting depth of a command context for
    Markdown title generation."""
    # Start from 1 to avoid H1 title
    depth = 1
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
    "row-policy": ["project", "table"],
    "view": ["project", "table"],
    "column": ["project", "table"],
    "kinesis": ["project", "table"],
    "kafka": ["project", "table"],
    "siem": ["project", "table"],
    "stream": ["project", "table"],
}


def _generate_options_table(command: click.Command, ctx: click.Context) -> str:
    """Generates a Markdown table for a command's options."""
    opts = [
        p.get_help_record(ctx)
        for p in command.get_params(ctx)
        if p.get_help_record(ctx) and "--help" not in p.opts
    ]

    if not opts:
        return ""

    format_args = _create_format_args(ctx)
    table_parts = [
        "**Options**",
        "",  # Adds a blank line before the table
        "| Option | Description |",
        "|:-------|:------------|",
    ]
    for opt, desc in opts:
        try:
            formatted_desc = desc.format(**format_args)
        except KeyError:
            formatted_desc = desc
        sanitized_desc = formatted_desc.replace("|", "\\|").replace("\n", " ")
        sanitized_opt = opt.replace("|", "\\|")
        table_parts.append(f"| `{sanitized_opt}` | {sanitized_desc} |")

    table_parts.append("")  # Adds a blank line after the table
    return "\n".join(table_parts)


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
        "resource": resource.replace("-", " "),
        "resource_plural": _inflect_engine.plural(singular_form).replace("-", " "),
        "parent_command": ctx.parent.command_path if ctx.parent else "hdxcli",
        "example_name": f"my_{resource.replace('-', '_')}",
        "full_command_prefix": _get_full_command_prefix(ctx),
    }


def _parse_docstring_for_markdown(docstring: str, format_args: dict) -> (str, str, str):
    """Parses a docstring specifically for markdown generation."""
    if not docstring:
        return "", "", ""

    cleaned_doc = inspect.cleandoc(docstring)

    parts = cleaned_doc.split("\f", 1)
    doc_part = parts[0]
    markdown_only_part = parts[1] if len(parts) > 1 else ""

    try:
        formatted_doc = doc_part.format(**format_args)
    except KeyError:
        formatted_doc = doc_part

    example_parts = re.split(r"\n\s*Examples?:\s*\n", formatted_doc, 1, re.IGNORECASE)
    description = example_parts[0].strip()
    examples = example_parts[1].strip() if len(example_parts) > 1 else ""

    # Fix double newlines in examples
    examples = examples.replace("\b\n", "")

    paragraphs = description.strip("\b\n").split("\n\n")
    paragraphs = [p for p in paragraphs if p.strip()]

    processed_paragraphs = []
    for p in paragraphs:
        if "\b" in p:
            processed_paragraphs.append(p.replace("\b\n", ""))
        else:
            processed_paragraphs.append(re.sub(r"\s+", " ", p.strip()))
    description = "\n\n".join(processed_paragraphs)

    return description, examples, markdown_only_part


class HdxCommand(click.Command):
    """A custom click.Command."""

    def to_markdown(self, ctx: click.Context) -> str:
        resource = ctx.parent.command.name if ctx.parent else self.name
        for param in self.get_params(ctx):
            if isinstance(param, click.Argument) and param.name == "resource_name":
                param.metavar = f"{resource.replace('-', '_').upper()}_NAME"

        depth = _get_depth(ctx)
        heading = "#" * depth
        command_title = self.name.replace("-", " ").replace("_", " ").title()
        md_parts = [f"{heading} {command_title}\n"]

        format_args = _create_format_args(ctx)
        description, examples_str, markdown_only_content = _parse_docstring_for_markdown(
            self.help, format_args
        )

        # Description
        if description:
            md_parts.append(f"{description}\n")

        # Usage
        usage_line = self.get_usage(ctx).replace("Usage: ", "")
        # if usage_line is too long, click adds '\n' at some point. It cleans it up.
        usage_line = re.sub(r"\s*\n\s*", " ", usage_line).strip()
        md_parts.append(f"**Usage**\n\n```bash\n{usage_line}\n```\n")

        # Options
        md_parts.append(_generate_options_table(self, ctx))

        # Examples
        if examples_str:
            md_parts.append("**Examples**\n")
            md_parts.append(f"```bash\n{inspect.cleandoc(examples_str)}\n```\n")

        # Markdown-only content
        if markdown_only_content:
            md_parts.append(f"{inspect.cleandoc(markdown_only_content)}\n")

        return "\n".join(md_parts)

    def get_help(self, ctx: click.Context) -> str:
        """Formats the command's help text, letting Click handle truncation via '\\f'."""
        # Update metavar before calling super().get_help()
        resource = ctx.parent.command.name if ctx.parent else self.name
        for param in self.params:
            if isinstance(param, click.Argument) and param.name == "resource_name":
                param.metavar = f"{resource.replace('-', '_').upper()}_NAME"

        help_text = super().get_help(ctx)
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
        heading = "#" * depth
        group_title = self.name.replace("_", " ").title()
        md_parts = [f"{heading} {group_title}\n"]

        format_args = _create_format_args(ctx)
        description, _, markdown_only_content = _parse_docstring_for_markdown(
            self.help, format_args
        )

        # Description
        if description:
            md_parts.append(f"{description}\n")

        # Usage
        usage_line = self.get_usage(ctx).replace("Usage: ", "")
        # if usage_line is too long, click adds '\n' at some point. It cleans it up.
        usage_line = re.sub(r"\s*\n\s*", " ", usage_line).strip()
        md_parts.append(f"**Usage**\n\n```bash\n{usage_line}\n```\n")

        # Options
        md_parts.append(_generate_options_table(self, ctx))

        # Markdown-only content
        if markdown_only_content:
            md_parts.append(f"{inspect.cleandoc(markdown_only_content)}\n")

        direct_commands_md, subgroups_md = [], []
        for cmd_name in self.list_commands(ctx):
            cmd = self.get_command(ctx, cmd_name)
            if not cmd or cmd.hidden:
                continue

            sub_ctx = click.Context(cmd, info_name=cmd_name, parent=ctx)
            cmd_md = cmd.to_markdown(sub_ctx)
            (subgroups_md if isinstance(cmd, click.Group) else direct_commands_md).append(cmd_md)

        if direct_commands_md:
            md_parts.append("\n\n".join(direct_commands_md))

        if subgroups_md:
            if direct_commands_md:
                md_parts.append("\n\n")

            md_parts.append("\n\n".join(subgroups_md))

        return "\n".join(md_parts)

    def get_help(self, ctx: click.Context) -> str:
        """Formats the group's help text, letting Click handle truncation via '\\f'."""
        help_text = super().get_help(ctx)
        if not help_text:
            return ""

        format_args = _create_format_args(ctx)
        try:
            return help_text.format(**format_args)
        except KeyError as e:
            logger.debug(f"Error formatting help for group '{self.name}': {e}.")
            return help_text
