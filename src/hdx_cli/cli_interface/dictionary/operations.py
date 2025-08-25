import json
import os

import click

from hdx_cli.cli_interface.common.undecorated_click_commands import basic_get
from hdx_cli.library_api.common.exceptions import HttpException
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.file_handling import write_bytes_to_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()


def download_dictionary_file(
    profile: ProfileUserContext,
    files_resource_path: str,
    dictionary_filename: str,
    output_path: str = None,
):
    """Downloads a dictionary file using the dedicated download endpoint."""
    try:
        download_url = f"{files_resource_path}/{dictionary_filename}/download"
        content = basic_get(profile, download_url, fmt="verbatim")
        logger.debug(f"Successfully downloaded '{dictionary_filename}'")
    except HttpException as e:
        if hasattr(e, "error_code") and e.error_code == 404:
            # Default error message
            error_message = f"File with name '{dictionary_filename}' not found."
            try:
                error_body = json.loads(e.message.decode("utf-8"))
                # Check for the specific message that indicates a missing endpoint
                if "Not found" in error_body.get("detail", ""):
                    error_message = (
                        "The download feature may not be available on this Hydrolix version."
                    )
            except (json.JSONDecodeError, AttributeError):
                # Invalid response format
                pass
            raise click.ClickException(error_message)

        # Any other HTTP error
        raise

    if output_path:
        final_path = output_path
    else:
        final_path = os.path.join(os.getcwd(), dictionary_filename)

    write_bytes_to_file(final_path, content)
    logger.info(f"Dictionary file saved to: {final_path}")
