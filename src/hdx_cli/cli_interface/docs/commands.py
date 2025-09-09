import click
from pathlib import Path
from importlib.metadata import version, PackageNotFoundError

from hdx_cli.cli_interface.common.click_extensions import HdxCommand
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit
from .config import DOC_STRUCTURE

logger = get_logger()

try:
    VERSION = version("hdxcli")
except PackageNotFoundError:
    VERSION = "0.0.0-dev"


@click.command(cls=HdxCommand, name="docs")
@click.argument(
    'target_dir',
    type=click.Path(file_okay=False, writable=True, resolve_path=True),
    required=True,
)
@click.argument('resource', metavar="RESOURCE_NAME", required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def docs(ctx: click.Context, target_dir: str, resource: str = None):
    """
    Generate and export CLI documentation in Markdown format.

    \b
    TARGET_DIR: The directory where the .md files will be saved.
    RESOURCE_NAME: (Optional) The specific command or group to document. If omitted, all documentation is generated.

    \b
    Examples:
      # Generate the complete documentation in the current directory
      hdxcli docs .

    \b
      # Generate documentation only for the 'project' command
      hdxcli docs ./cli_docs project

    \b
      # Generate documentation for the 'kinesis' subgroup
      hdxcli docs /tmp/docs kinesis
    """
    output_path = Path(target_dir)
    output_path.mkdir(exist_ok=True)
    root_cli = ctx.find_root().command
    root_ctx = click.Context(root_cli, info_name="hdxcli")

    if resource:
        _generate_single_resource_doc(root_ctx, resource, output_path)
    else:
        _generate_full_docs(root_ctx, output_path)


def _generate_full_docs(root_ctx: click.Context, output_path: Path):
    """Generates the full set of structured documentation files."""
    logger.info(f"Starting full documentation export to '{output_path}'...")

    for doc_group in DOC_STRUCTURE:
        group_filename = doc_group["filename"]
        command_names = doc_group["commands"]
        frontmatter_data = doc_group.get("frontmatter", {})

        # frontmatter
        frontmatter_parts = ["---"]
        for key, value in frontmatter_data.items():
            if isinstance(value, bool):
                frontmatter_parts.append(f'{key}: {str(value).lower()}')
            else:
                frontmatter_parts.append(f'{key}: "{value}"')
        frontmatter_parts.append("---")

        frontmatter = "\n".join(frontmatter_parts)
        version_line = f"> _hdxcli v{VERSION}_"

        # The markdown file starts with the frontmatter and the version line
        final_md_content = f"{frontmatter}\n\n{version_line}\n\n"

        body_parts = []
        for command_name in command_names:
            command = root_ctx.command.get_command(root_ctx, command_name)
            if command and hasattr(command, 'to_markdown'):
                cmd_ctx = click.Context(command, info_name=command_name, parent=root_ctx)
                md_content = command.to_markdown(cmd_ctx)
                body_parts.append(md_content)

        final_md_content += "\n\n---\n\n".join(body_parts)

        file_path = output_path / group_filename
        file_path.write_text(final_md_content, encoding="utf-8")
        logger.info(f"Successfully generated '{file_path}'.")
    logger.info("Full documentation export complete.")


def _generate_single_resource_doc(root_ctx: click.Context, resource_name: str, output_path: Path):
    """Generates a single markdown file for a specific resource."""
    logger.info(f"Exporting documentation for '{resource_name}' to '{output_path}'...")

    command, cmd_ctx = _find_command_recursively(root_ctx, resource_name)

    if not command:
        raise click.UsageError(f"Error: Command or group '{resource_name}' not found.")

    if hasattr(command, 'to_markdown'):
        md_content = command.to_markdown(cmd_ctx)
        version_line = f"> _hdxcli v{VERSION}_"

        lines = md_content.split('\n')
        lines.insert(1, f"\n{version_line}\n")
        final_md_content = '\n'.join(lines)

        file_path = output_path / f"{resource_name}.md"
        file_path.write_text(final_md_content, encoding="utf-8")
        logger.info(f"Successfully generated documentation for '{resource_name}' at '{file_path}'.")
    else:
        logger.warning(f"Warning: Resource '{resource_name}' does not support documentation export.")


def _find_command_recursively(ctx: click.Context, name: str):
    """Recursively search for a command or group by name."""
    for cmd_name in ctx.command.list_commands(ctx):
        if cmd_name == name:
            command = ctx.command.get_command(ctx, cmd_name)
            return command, click.Context(command, info_name=cmd_name, parent=ctx)

    # If not found, recurse into subgroups
    for cmd_name in ctx.command.list_commands(ctx):
        command = ctx.command.get_command(ctx, cmd_name)
        if isinstance(command, click.Group):
            sub_ctx = click.Context(command, info_name=cmd_name, parent=ctx)
            found_cmd, found_ctx = _find_command_recursively(sub_ctx, name)
            if found_cmd:
                return found_cmd, found_ctx

    return None, None
