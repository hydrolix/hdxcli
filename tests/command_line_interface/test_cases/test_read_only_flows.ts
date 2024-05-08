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
                "python3 -m hdx_cli.main table --project test_ci_project create test_ci_table",
                "python3 -m hdx_cli.main table --project test_ci_project create test_ci_table_ingest",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/transform_settings.json test_ci_transform",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table_ingest create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/csv_transform.json test_csv_transform",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table_ingest create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/json_transform.json test_json_transform",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table_ingest create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/gzip_transform.json test_gzip_transform",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table_ingest create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/zip_transform.json test_zip_transform",
                "python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table_ingest create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/zlib_transform.json test_zlib_transform",
                "python3 -m hdx_cli.main dictionary --project test_ci_project files upload -t verbatim {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_file.csv test_ci_dictionary_file",
                "python3 -m hdx_cli.main dictionary --project test_ci_project create {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_settings.json test_ci_dictionary_file test_ci_dictionary",
                "python3 -m hdx_cli.main job batch --project test_ci_project --table test_ci_table ingest test_ci_batch_job {HDXCLI_TESTS_DIR}/tests_data/batch-jobs/batch_job_ci_settings.json",
                "python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table create {HDXCLI_TESTS_DIR}/tests_data/sources/kafka_source_settings.json test_ci_kafka_source",
                "python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table create {HDXCLI_TESTS_DIR}/tests_data/sources/kinesis_source_settings.json test_ci_kinesis_source",
                "python3 -m hdx_cli.main sources siem --project test_ci_project --table test_ci_table create {HDXCLI_TESTS_DIR}/tests_data/sources/siem_source_settings.json test_ci_siem_source",
                "python3 -m hdx_cli.main function --project test_ci_project create -s '(x,k,b)->k*x+b' test_ci_function",
                "python3 -m hdx_cli.main storage create -f {HDXCLI_TESTS_DIR}/tests_data/storages/storage_ci_settings.json test_ci_storage",
                "python -m hdx_cli.main role create --name test_ci_role --permission change_table",
                "python -m hdx_cli.main user invite send test_ci_invite_user@hydolix.io --role test_ci_role",
                "python3 -m hdx_cli.main unset"
                ]

global_teardown = ["python3 -m hdx_cli.main storage delete --disable-confirmation-prompt test_ci_storage",
                   "python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_ci_project",
                   "python -m hdx_cli.main role delete --disable-confirmation-prompt test_ci_role",
                   "python -m hdx_cli.main user delete --disable-confirmation-prompt test_ci_invite_user@hydolix.io",
                   "python3 -m hdx_cli.main unset"]



######################################################## Project ########################################################
[[test]]
name = "Projects can be created"
commands_under_test = ["python3 -m hdx_cli.main project create test_project"]
teardown = ["python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_project"]
expected_output = 'Created project test_project'

[[test]]
name = "Projects can be deleted"
setup = ["python3 -m hdx_cli.main project create test_project"]
commands_under_test = ["python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_project"]
expected_output = 'Deleted test_project'

[[test]]
name = "Projects can be listed"
commands_under_test = ["python3 -m hdx_cli.main project list"]
expected_output_re = '.*?test_ci_project.*'

[[test]]
name = "Project settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main project --project test_ci_project settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_project" in result'

[[test]]
name = "Project description can be modified"
commands_under_test = ["python3 -m hdx_cli.main project --project test_ci_project settings description 'Modified-with-hdxcli-tool'"]
teardown = ["python3 -m hdx_cli.main project --project test_ci_project settings description 'Created-with-hdxcli-tool'"]
expected_output = 'Updated test_ci_project description'

[[test]]
name = "Projects can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main project show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "org" in result and "uuid" in result and "test_ci_project" in result'

[[test]]
name = "Project statistics can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main project stats"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "total_partitions" in result and "total_storage_size" in result and "test_ci_project" in result'

#[[test]]
#name = "Project activities can be shown"
#commands_under_test = ["python3 -m hdx_cli.main project --project test_ci_project activity"]
#expected_output_expr = 'not result.startswith("Error:") and "created" in result and "test_ci_project" in result'


######################################################### Table #########################################################
[[test]]
name = "Tables can be created"
setup = ["python3 -m hdx_cli.main unset"]
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project create test_table"]
teardown = ["python3 -m hdx_cli.main table --project test_ci_project delete --disable-confirmation-prompt test_table"]
expected_output = 'Created table test_table'

[[test]]
name = "Tables can be deleted"
setup = ["python3 -m hdx_cli.main set test_ci_project",
		     "python3 -m hdx_cli.main table create test_table"]
commands_under_test = ["python3 -m hdx_cli.main table delete --disable-confirmation-prompt test_table"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Deleted test_table'

[[test]]
name = "Tables can be listed"
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project list"]
expected_output_re = '.*?test_ci_table.*'

[[test]]
name = "Tables can be truncated"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main table truncate test_ci_table"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Truncated table test_ci_table'

[[test]]
name = "Table settings can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main table settings"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_table" in result'

[[test]]
name = "Table description can be modified"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main table settings description 'Modified-with-hdxcli-tool'"]
teardown = ["python3 -m hdx_cli.main table settings description 'Created-with-hdxcli-tool'",
			      "python3 -m hdx_cli.main unset"]
expected_output = 'Updated test_ci_table description'

[[test]]
name = "Table merge setting can be modified"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main table settings settings.merge.enabled false"]
teardown = ["python3 -m hdx_cli.main table settings settings.merge.enabled true",
			      "python3 -m hdx_cli.main unset"]
expected_output = 'Updated test_ci_table settings.merge.enabled'

[[test]]
name = "Tables can be shown"
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project --table test_ci_table show"]
expected_output_expr = 'not result.startswith("Error:") and "project" in result and "name" in result and "uuid" in result and "settings" in result and "test_ci_table" in result'

[[test]]
name = "Table statistics can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main table stats"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "total_partitions" in result and "total_storage_size" in result and "test_ci_table" in result'

[[test]]
name = "Table activities can be shown"
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project --table test_ci_table activity"]
expected_output_expr = 'not result.startswith("Error:") and "created" in result and "test_ci_table" in result'


#################################################### Summary Table #####################################################

######################################################## User #########################################################
[[test]]
name = "Users can be listed"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user list"]
expected_output_re = '.*?user_invite_cli@hydrolix.io                  super_admin.*'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Assign role to user"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user assign-role user_invite_cli@hydrolix.io -r read_only"]
expected_output = 'Added role(s) to user_invite_cli@hydrolix.io'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Assign role to non-exist user"
commands_under_test = ["python3 -m hdx_cli.main user assign-role user_non-exist@hydrolix.io -r read_only"]
expected_output = 'Error: Cannot find resource.'

[[test]]
name = "Assign non-exist role to user"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user assign-role user_invite_cli@hydrolix.io -r role_non-exist"]
expected_output = 'Error: Object with name=role_non-exist does not exist.'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Remove role to user"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user remove-role user_invite_cli@hydrolix.io -r super_admin"]
expected_output = 'Removed role(s) from user_invite_cli@hydrolix.io'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Remove non-exist role to user"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user remove-role user_invite_cli@hydrolix.io -r inexistent"]
expected_output = "Error: User user_invite_cli@hydrolix.io lacks ['inexistent'] role(s) for removal."
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Delete resource"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]
expected_output = 'Deleted user_invite_cli@hydrolix.io'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Delete not exist resource"
commands_under_test = ["python3 -m hdx_cli.main user remove-role not_exist@hydrolix.io -r super_admin"]
expected_output = 'Error: Cannot find resource.'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "User can be show"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user --user user_invite_cli@hydrolix.io show"]
expected_output_re = '.*?"email": "user_invite_cli@hydrolix.io".*'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Send invitation to a user"
commands_under_test = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
expected_output_re = 'Sent invitation to user_invite_cli@hydrolix.io'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Send invitation to a user already exist"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
expected_output_re = 'Error: User already exists. if trying to resend an invite, use /invites/<invite_id>/resend_invite.'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Resend invitation to a user"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user invite resend user_invite_cli@hydrolix.io"]
expected_output_re = 'Resent invitation to user_invite_cli@hydrolix.io'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Resend invitation to a not exist user"
commands_under_test = ["python3 -m hdx_cli.main user invite resend user_not_exist@hydrolix.io"]
expected_output_re = 'Error: Cannot find resource.'

[[test]]
name = "Invitation list"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user invite list"]
expected_output_re = '.*?user_invite_cli@hydrolix.io                  pending.*'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Delete the invite"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]
expected_output_re = 'Deleted user_invite_cli@hydrolix.io'

[[test]]
name = "Delete the invite with user inexistent"
commands_under_test = ["python3 -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]
expected_output_re = 'Could not delete user_invite_cli@hydrolix.io'

[[test]]
name = "Invite can be show"
setup = ["python3 -m hdx_cli.main user invite send user_invite_cli@hydrolix.io -r super_admin"]
commands_under_test = ["python3 -m hdx_cli.main user invite --user user_invite_cli@hydrolix.io show"]
expected_output_re = '.*?{"email": "user_invite_cli@hydrolix.io".*'
teardown = ["python -m hdx_cli.main user invite delete user_invite_cli@hydrolix.io --disable-confirmation-prompt"]

[[test]]
name = "Invite to a user not exist can be show"
commands_under_test = ["python3 -m hdx_cli.main user invite --user user_not_exist@hydrolix.io show"]
expected_output_re = 'Error: Cannot find resource.'
##################################################### Set/Unset ######################################################
[[test]]
name = "Set can be used"
commands_under_test = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*? set project/table*'

[[test]]
name = "Unset can be used"
commands_under_test = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*? unset project/table*'
