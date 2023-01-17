import click
import toml

from ...library_api.common.exceptions import HdxCliException
from ...library_api.utility.decorators import report_error_and_exit
from hdx_cli.library_api.common.config_constants import HDX_CLI_HOME_DIR, PROFILE_CONFIG_FILE
from hdx_cli.library_api.common.profile import save_profile, get_profile_data_from_standard_input

@click.group(help="Profile-related operations")
@click.pass_context
def profile(ctx: click.Context):
    ctx.obj = {'usercontext': ctx.parent.obj['usercontext']}



@click.command(help='Show profile')
@click.argument('profile_name', default=None, required=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_show(ctx: click.Context,
                 profile_name):
    profilename = ctx.parent.obj['usercontext'].profilename if not profile_name else profile_name
    print(f'Showing [{profilename}]')
    print(f'-' * 100)
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
        for cfg_name, cfg_val in cfg_dict.items():
            if cfg_name == profilename:
                print(f"Profile: {cfg_name}")
                for cfg_key, cfg_val in cfg_val.items():
                    print(f"{cfg_key}: {cfg_val}")


@click.command(help='List profiles')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_list(ctx: click.Context):
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
        for cfg_name in cfg_dict:
            print(cfg_name)


@click.command(help='Edit profile')
@click.argument('profile_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_edit(ctx: click.Context,
                 profile_name: str):
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
    profile_to_edit = cfg_dict[profile_name]
    print(f'Editing [{profile_name}]')
    print(f'-' * 100)
    username, hostname, scheme = (profile_to_edit['username'],
                                  profile_to_edit['hostname'],
                                  profile_to_edit['scheme'])

    edit_profile_data = get_profile_data_from_standard_input(hostname=hostname,
                                                             username=username,
                                                             http_scheme=scheme)
    save_profile(username=edit_profile_data.username,
                 hostname=edit_profile_data.hostname,
                 profile_config_file=PROFILE_CONFIG_FILE,
                 profilename=profile_name,
                 scheme=edit_profile_data.scheme,
                 initial_profile=cfg_dict)


@click.command(help='Add profile')
@click.argument('profile_name')
@click.pass_context
@report_error_and_exit(exctype=Exception)
def profile_add(ctx: click.Context,
                profile_name: str):
    with open(PROFILE_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        cfg_dict = toml.load(config_file)
    if cfg_dict.get(profile_name):
        print(f"Error: profile '{profile_name}' already exists")
        return

    edit_profile_data = get_profile_data_from_standard_input()
    save_profile(username=edit_profile_data.username,
                 hostname=edit_profile_data.hostname,
                 profile_config_file=PROFILE_CONFIG_FILE,
                 profilename=profile_name,
                 scheme=edit_profile_data.scheme,
                 initial_profile=cfg_dict)


profile.add_command(profile_list, name='list')
profile.add_command(profile_show, name='show')
profile.add_command(profile_edit, name='edit')
profile.add_command(profile_add, name='add')
