# Test suite example
#
# Every [[test]] has
#
# - name: description of the test
# - input_commands: a set of input commands
# - one of (expected_output, expected_output_re, expected_output_expr): it checks if the result is correct
# - teardown: list with teardown commands

[[test]]
name = "Projects can be listed"
input_commands = ["python3 -m hdx_cli.main project list"]
# Only last command output is checked
expected_output_re = '.*?german.*'


[[test]]
name = "Tables cannot be listed when project not set"
input_commands = ["python3 -m hdx_cli.main table list"]
# Only last command output is checked
expected_output_expr = 'result.startswith("Error:")'


[[test]]
name = "Tables can be listed when project is set"
setup = ["python3 -m hdx_cli.main set german booleans_table"]
input_commands = ["python3 -m hdx_cli.main table list"]
# Only last command output is checked
expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
teardown = ["python3 -m hdx_cli.main unset"]
