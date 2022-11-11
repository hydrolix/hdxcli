# The global test suite can have keys:
#
# global_setup (list, optional) -> commands to set up before starting tests
# global_teardown (list, optional) ->  commands to tear down resources after executing tests
#

# Every [[test]] has:
#
# - name (string, required): description of the test
# - commands_under_test (list, required): an single input command, specified as a toml list
#
# - one of (expected_output, expected_output_re, expected_output_expr), (string, optional):
#   it checks if the result is correct, according to the following logic:
#
#   1. if expected_output is used, the output is compared verbatim against commands_under_test command
#   2. if expected_output_re is used, the outputs is compared against a regular expression. Note that dots
#      match anything, including new lines
#   3. if expected_output_expr is used, you can use the variable 'result' with an arbitrarty python expression.
#      result is a string. For example: 'result.startswith("Tables:") and "mytable" in result'
#
# - setup (optional): a setup list of commands executed before running the commands_under_test
# - teardown (optional): list with teardown commands
global_setup = ["python3 -m hdx_cli.main project create test_ci_project",
                "python3 -m hdx_cli.main --project test_ci_project table create test_ci_table",
                "python3 -m hdx_cli.main --project test_ci_project --table test_ci_table transform create -f {HDXCLI_TESTS_DIR}/tests_data/every_datatype_transform.json test_ci_transform",
                "python3 -m hdx_cli.main set test_ci_project test_ci_table"]

global_teardown = ["python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_ci_project",
                   "python3 -m hdx_cli.main unset"]


[[test]]
name = "Projects can be listed"
commands_under_test = ["python3 -m hdx_cli.main project list"]
expected_output_re = '.*?test_ci_project.*'

[[test]]
name = "Tables can be listed with explicit project"
commands_under_test = ["python3 -m hdx_cli.main --project test_ci_project table list"]
expected_output_re = '.*?test_ci_table.*'


[[test]]
name = "Tables can be listed with explicit project without preset table and project"
setup = ["python3 -m hdx_cli.main unset"]
commands_under_test = ["python3 -m hdx_cli.main --project test_ci_project table list"]
teardown = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
expected_output_re = '.*?test_ci_table.*'


[[test]]
name = "Tables can be listed without explicit project with preset table and project"
commands_under_test = ["python3 -m hdx_cli.main --project test_ci_project table list"]
expected_output_re = '.*?test_ci_table.*'


[[test]]
name = "Transforms can be listed "
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main transform list"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?test_ci_transform.*'


[[test]]
name = "Transforms settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main --transform test_ci_transform transform settings"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?.*'

[[test]]
name = "Inexistent Table cannot be deleted"
commands_under_test = ["python3 -m hdx_cli.main --project test_ci_project table delete --disable-confirmation-prompt a_table_that_does_not_exist"]
expected_output_re = 'Could not delete.*?a_table_that_does_not_exist.*'



# [[test]]
# name = "A transform can be deleted"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Table settings can be read"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Table settings can be written"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Table settings can be shown"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Project settings can be read"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Project settings can be written"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Project settings can be shown"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]



# [[test]]
# name = "Transform settings can be read"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Transform settings can be written"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]


# [[test]]
# name = "Transform can be shown"
# # setup = ["python3 -m hdx_cli.main set german booleans_table"]
# # commands_under_test = ["python3 -m hdx_cli.main table list"]
# # expected_output_expr = 'not result.startswith("Error:") and "booleans_table" in result'
# # teardown = ["python3 -m hdx_cli.main unset"]
