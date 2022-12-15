import click
import toml

from ...library_api.common.exceptions import HdxCliException
from ...library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.common.config_constants import HDX_CLI_HOME_DIR, PROFILE_CONFIG_FILE


@click.group(help="Profile-related operations")
@click.pass_context
def profile(ctx: click.Context):
    ctx.obj = {'usercontext': ctx.parent.obj['usercontext']}

@click.command(help='List profiles')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_show(ctx: click.Context):
    profilename = ctx.parent.obj['usercontext'].profilename
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
        for cfg_name, cfg_val in cfg_dict.items():
            if cfg_name == profilename:
                print(f"Profile: {cfg_name}")
                for cfg_key, cfg_val in cfg_val.items():
                    print(f"{cfg_key}: {cfg_val}")

@click.command(help='Show profile')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_list(ctx: click.Context):
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
        for cfg_name in cfg_dict:
            print(cfg_name)


profile.add_command(profile_list, name='list')
profile.add_command(profile_show, name='show')

