import click

from .kafka import kafka as command_kafka
from .kinesis import kinesis as command_kinesis
from .siem import siem as command_siem
from hdx_cli.cli_interface.common.click_extensions import HdxGroup


@click.group(cls=HdxGroup)
def source():
    """Manage sources (Kafka, Kinesis, SIEM)."""


source.add_command(command_kafka)
source.add_command(command_kinesis)
source.add_command(command_siem)
