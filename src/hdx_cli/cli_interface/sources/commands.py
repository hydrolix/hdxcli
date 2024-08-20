"""Commands relative to sources handling  operations"""
import click

from .kafka import kafka as command_kafka
from .kinesis import kinesis as command_kinesis
from .summary import summary as command_summary
from .siem import siem as command_siem


@click.group(help="Sources-related operations")
def sources():
    pass


sources.add_command(command_kafka)
sources.add_command(command_kinesis)
sources.add_command(command_summary)
sources.add_command(command_siem)
