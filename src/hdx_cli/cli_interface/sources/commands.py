"""Commands relative to sources handling  operations"""

import click

from hdx_cli.library_api.utility.decorators import report_error_and_exit

from .kafka import kafka as command_kafka
from .kinesis import kinesis as command_kinesis
from .siem import siem as command_siem


@click.group(help="Sources-related operations")
@report_error_and_exit(exctype=Exception)
def sources():
    pass


sources.add_command(command_kafka)
sources.add_command(command_kinesis)
sources.add_command(command_siem)
