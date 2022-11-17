from enum import Enum
import os
import re
import sys

from pathlib import Path
import subprocess as sp
from typing import List, Tuple
from functools import partial

import toml
import pytest

from hdx_cli.library_api.common.exceptions import (
    BadFileNameConventionException,
    HdxCliException)

HDXCLI_TESTS_CLUSTER_USERNAME = os.getenv('HDXCLI_TESTS_CLUSTER_USERNAME')
HDXCLI_TESTS_CLUSTER_HOSTNAME = os.getenv('HDXCLI_TESTS_CLUSTER_HOSTNAME')
HDXCLI_TESTS_CLUSTER_PASSWORD = os.getenv('HDXCLI_TESTS_CLUSTER_PASSWORD')

THIS_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
HDXCLI_PROFILE_DATA_FILE = THIS_DIR / 'config_for_testing.toml'


def _create_cluster_config_file_for_tests():
    if not HDXCLI_TESTS_CLUSTER_USERNAME or not HDXCLI_TESTS_CLUSTER_HOSTNAME:
        raise RuntimeError('HDXCLI_TESTS_CLUSTER_USERNAME and HDXCLI_TESTS_CLUSTER_HOSTNAME'
                           ' environment vars must be set')
    config = {'default': {'username': HDXCLI_TESTS_CLUSTER_USERNAME,
                          'hostname': HDXCLI_TESTS_CLUSTER_HOSTNAME}}
    with open(HDXCLI_PROFILE_DATA_FILE, 'w+', encoding='utf-8') as cfile:
        toml.dump(config, cfile)


_create_cluster_config_file_for_tests()


HDXCLI_TEST_CASES_DIR = THIS_DIR / 'test_cases'
HDXCLI_PROFILE_DATA = toml.load(open(HDXCLI_PROFILE_DATA_FILE, 'r',
                                     encoding='utf-8'))


def _load_toml_file_with_extension(toml_file_path, expected_extension=''):
    if not (fname := Path(toml_file_path).name).endswith(expected_extension):
        raise BadFileNameConventionException(
            f'Filename with ending in {expected_extension}'
            f' expected. Got {fname}')
    with open(toml_file_path, 'r', encoding='utf-8') as toml_file:
        return toml.load(toml_file)

class ExpectedOutput(Enum):
    VERBATIM = 0
    REGEX    = 1
    EXPRESSION = 2


def _assert_test_output(result, expected_output_tpl: Tuple[ExpectedOutput, str]):
    if expected_output_tpl[0] == ExpectedOutput.VERBATIM:
        assert result == expected_output_tpl[1]
    elif expected_output_tpl[0] == ExpectedOutput.REGEX:
        assert re.match(expected_output_tpl[1], result, re.DOTALL)
    elif expected_output_tpl[0] == ExpectedOutput.EXPRESSION:
        assert eval(expected_output_tpl[1], {'result': result}) # pylint:disable=eval-used


def _add_parameters_to_hdx_cli_command(cmd: str):
        split_cmd = cmd.split(' ')
        idx = split_cmd.index('hdx_cli.main') + 1
        split_cmd.insert(idx, '--profile-config-file')
        idx += 1
        split_cmd.insert(idx, str(HDXCLI_PROFILE_DATA_FILE))
        if HDXCLI_TESTS_CLUSTER_PASSWORD:
            idx += 1
            split_cmd.insert(idx, '--password')
            idx += 1
            split_cmd.insert(idx, HDXCLI_TESTS_CLUSTER_PASSWORD)
        return split_cmd

Command = List[str]

def _add_parameters_to_hdx_cli_commands(cmds: List[str]) -> List[Command]:
    return [_add_parameters_to_hdx_cli_command(cmd) for cmd in cmds]


def _execute_commands(commands: List[Command], check=True):
    for command in commands:
        sp.run(command, check=check)


class ApprovalTestCaseRunner:
    def __init__(self, name, input_commands, expected_output_tpl: Tuple[ExpectedOutput, str],
                *, setup_steps=None, teardown_steps=None):
        self._name = name

        setup_steps = setup_steps if setup_steps else []
        teardown_steps = teardown_steps if teardown_steps else []

        self._expected_output_tpl = expected_output_tpl
        self._input_commands = []
        self._setup_steps = []
        self._teardown_steps = []

        for cmd in input_commands:
            split_cmd = _add_parameters_to_hdx_cli_command(cmd)
            self._input_commands.append(split_cmd)

        for cmd in setup_steps:
            split_cmd = _add_parameters_to_hdx_cli_command(cmd)
            self._setup_steps.append(split_cmd)

        for cmd in teardown_steps:
            split_cmd = _add_parameters_to_hdx_cli_command(cmd)
            self._teardown_steps.append(split_cmd)


    def run(self):
        """Execute an approval test. The approval test is a set of input commands and
        and expected output. The last input command output is checked against the
        expected output. Using several steps is supported in case you need to
        execute some configuration steps before
        being able to run the command you want to test.
        """
        input_commands = self._input_commands
        expected_output_tpl = self._expected_output_tpl
        result = None
        command_capture = None
        try:
            for command in self._setup_steps:
                command_capture = ' '.join(command)
                result_obj = sp.run(command, capture_output=True, check=True)
            for command in input_commands:
                command_capture = ' '.join(command)
                result_obj = sp.run(command, capture_output=True, check=False)
                result = result_obj.stdout.rstrip().decode('utf-8')
        except HdxCliException:
            pytest.fail(
                f"An exception occurred when trying to run '{self._name}'. Command: "
                f"'{command_capture}'.")
        except Exception: # pylint:disable=broad-except
            pytest.fail(
                f"An exception occurred when trying to run '{self._name}'. Command: "
                f"'{command_capture}'.")
        finally:
            for command in self._teardown_steps:
                command_capture = ' '.join(command)
                result_obj = sp.run(command, capture_output=True, check=True)
        try:
            _assert_test_output(result, expected_output_tpl)
        except AssertionError as assert_error:
            print(f"Assertion error in test '{self._name}', Command: '{command_capture}'",file=sys.stderr)
            print(assert_error, file=sys.stderr)
            raise

def test_approval_run_all(test_data: Tuple[List[str], str, str]):

    tc_runner = ApprovalTestCaseRunner(name=test_data[2],
                                       input_commands=test_data[0],
                                       expected_output_tpl=test_data[1],
                                       setup_steps=test_data[3],
                                       teardown_steps=test_data[4])
    tc_runner.run()


_load_test_suite_file = (
    partial(_load_toml_file_with_extension, expected_extension='.ts'))


THE_TESTS = _load_test_suite_file(
    HDXCLI_TEST_CASES_DIR / 'test_read_only_flows.ts')


def _parse_expected_output(tst):
    if val := tst.get('expected_output'):
        return (ExpectedOutput.VERBATIM, val)
    elif val := tst.get('expected_output_re'):
        return (ExpectedOutput.REGEX, val)
    elif val := tst.get('expected_output_expr'):
        return (ExpectedOutput.EXPRESSION, val)
    raise KeyError('No expected_output of any form found for test validation')


def pytest_generate_tests(metafunc):
    """Generate all tests from tests_data"""
    def interpolate_with_profile_vars(a_str):
        profile_dict = HDXCLI_PROFILE_DATA['default']
        profile_dict['HDXCLI_TESTS_DIR'] = THIS_DIR
        interpolated = a_str.format(**profile_dict)
        return interpolated

    global_setup = THE_TESTS.get('global_setup')
    if global_setup:
        global_setup_interpolated = [interpolate_with_profile_vars(tic)
                                 for tic in global_setup]
        global_setup_cmds = _add_parameters_to_hdx_cli_commands(global_setup_interpolated)
        _execute_commands(global_setup_cmds, check=True)
    tests_array = THE_TESTS['test']
    all_tests = []
    for tst in tests_array:
        test_input_commands = [interpolate_with_profile_vars(tic)
                            for tic in
                            tst['commands_under_test']]
        all_tests.append((test_input_commands, _parse_expected_output(tst), tst['name'], 
                          tst.get('setup', None),
                          tst.get('teardown', None)))

    global_teardown = THE_TESTS.get('global_teardown')

    # This is basically a trick: add a final test as the teardown test
    # bc pytest_generate_tests seems to not support global teardown
    all_tests.append((['python3 -m hdx_cli.main'], (ExpectedOutput.REGEX, '.*'), 'teardown', None, global_teardown))
    metafunc.parametrize("test_data", all_tests)
