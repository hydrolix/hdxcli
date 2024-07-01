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
expected_output_re = '.*?name                                                                                      string                        "test_ci_project".*'

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
expected_output_re = '.*?{"name": "test_ci_project", "org": "c90fcaee-6ccb-4948-a30b-84f86d8fb583", "description": "Created with hdxcli tool".*'

[[test]]
name = "Project statistics can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main project stats"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = '{"summary": {"name": "test_ci_project", "total_partitions": 0, "total_rows": 0, "total_data_size": 0, "total_storage_size": 0, "total_raw_data_size": 0}, "tables": []}'

[[test]]
name = "Project activities can be shown"
commands_under_test = ["python3 -m hdx_cli.main project --project test_ci_project activity"]
expected_output_re = '.*?{"name": "test_ci_project", "description": "Created with hdxcli tool"}}, "text": "Published config"}}}}.*'


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
[[test]]
name = "Summary (stream) tables can be created"
setup = ["python3 -m hdx_cli.main unset"]
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project create -t summary -f {HDXCLI_TESTS_DIR}/tests_data/summary/stream/sql.txt test_summary_table_stream"]
teardown = ["python3 -m hdx_cli.main table --project test_ci_project delete --disable-confirmation-prompt test_summary_table_stream"]
expected_output = 'Created table test_summary_table_stream'

[[test]]
name = "Summary (kafka) tables can be created"
setup = ["python3 -m hdx_cli.main unset"]
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project create -t summary -f {HDXCLI_TESTS_DIR}/tests_data/summary/kafka/sql.txt test_summary_table_kafka"]
teardown = ["python3 -m hdx_cli.main table --project test_ci_project delete --disable-confirmation-prompt test_summary_table_kafka"]
expected_output = 'Created table test_summary_table_kafka'

[[test]]
name = "Summary (kinesis) tables can be created"
setup = ["python3 -m hdx_cli.main unset"]
commands_under_test = ["python3 -m hdx_cli.main table --project test_ci_project create -t summary -f {HDXCLI_TESTS_DIR}/tests_data/summary/kinesis/sql.txt test_summary_table_kinesis"]
teardown = ["python3 -m hdx_cli.main table --project test_ci_project delete --disable-confirmation-prompt test_summary_table_kinesis"]
expected_output = 'Created table test_summary_table_kinesis'


###################################################### Transform #######################################################
[[test]]
name = "Transforms can be created"
setup = ["python3 -m hdx_cli.main project create test_ci_project_test",
        "python3 -m hdx_cli.main table --project test_ci_project_test create test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main transform --project test_ci_project_test --table test_ci_table create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/transform_settings.json test_transform"]
teardown = ["python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_ci_project_test"]
expected_output = 'Created transform test_transform'

[[test]]
name = "Transforms can be deleted"
setup = ["python3 -m hdx_cli.main project create test_ci_project_test",
         "python3 -m hdx_cli.main table --project test_ci_project_test create test_ci_table",
         "python3 -m hdx_cli.main set test_ci_project_test test_ci_table",
         "python3 -m hdx_cli.main transform create -f {HDXCLI_TESTS_DIR}/tests_data/transforms/transform_settings.json test_transform"]
commands_under_test = ["python3 -m hdx_cli.main transform delete --disable-confirmation-prompt test_transform"]
teardown = ["python3 -m hdx_cli.main unset",
            "python3 -m hdx_cli.main project delete --disable-confirmation-prompt test_ci_project_test"]
expected_output = 'Deleted test_transform'

[[test]]
name = "Transforms can be listed"
commands_under_test = ["python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table list"]
expected_output_re = '.*?test_ci_transform.*'

[[test]]
name = "Transform settings can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main transform --transform test_ci_transform settings"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_transform" in result'

#Error: (405, b\'{"detail":"Method \\\\"PATCH\\\\" not allowed."}\
#[[test]]
#name = "Transform type can be modified"
#commands_under_test = ["python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table --transform test_ci_transform settings type csv"]
#teardown = ["python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table --transform test_ci_transform settings type json"]
#expected_output = 'Updated test_ci_transform type'

[[test]]
name = "Transform settings.is_default can be shown"
commands_under_test = ["python3 -m hdx_cli.main transform --project test_ci_project --table test_ci_table --transform test_ci_transform settings settings.is_default"]
expected_output = 'settings.is_default: True'

[[test]]
name = "Transforms can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main transform --transform test_ci_transform show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "output_columns" in result and "test_ci_transform" in result and "test_ci_transform" in result'

## 'map-from' tests are missing.


######################################################### Kafka ########################################################
[[test]]
name = "Kafka sources can be created"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources kafka create {HDXCLI_TESTS_DIR}/tests_data/sources/kafka_source_settings.json test_kafka_source"]
teardown = ["python3 -m hdx_cli.main sources kafka delete --disable-confirmation-prompt test_kafka_source",
			      "python3 -m hdx_cli.main unset"]
expected_output = 'Created source test_kafka_source'

[[test]]
name = "Kafka sources can be deleted"
setup = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table create {HDXCLI_TESTS_DIR}/tests_data/sources/kafka_source_settings.json test_kafka_source"]
commands_under_test = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table delete --disable-confirmation-prompt test_kafka_source"]
expected_output = 'Deleted test_kafka_source'

[[test]]
name = "Kafka sources can be listed"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources kafka list"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?test_ci_kafka_source.*'

[[test]]
name = "Kafka source settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table --source test_ci_kafka_source settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result'

## failing
#[[test]]
#name = "Kafka source name can be modified"
#commands_under_test = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table --source test_ci_kafka_source settings name new_kafka_name"]
#teardown = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table --source new_kafka_name settings name test_ci_kafka_source"]
#expected_output = 'Updated new_kafka_name name'

[[test]]
name = "Kafka source bootstrap_servers can be shown"
commands_under_test = ["python3 -m hdx_cli.main sources kafka --project test_ci_project --table test_ci_table --source test_ci_kafka_source settings settings.bootstrap_servers"]
expected_output = "settings.bootstrap_servers: ['104.198.40.96:9092']"

[[test]]
name = "Kafka sources can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources kafka --source test_ci_kafka_source show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "\"subtype\": \"kafka\"" in result'


######################################################## Kinesis #######################################################
[[test]]
name = "Kinesis sources can be created"
commands_under_test = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table create {HDXCLI_TESTS_DIR}/tests_data/sources/kinesis_source_settings.json test_kinesis_source"]
teardown = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table delete --disable-confirmation-prompt test_kinesis_source"]
expected_output = 'Created source test_kinesis_source'

[[test]]
name = "Kinesis sources can be deleted"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table",
		     "python3 -m hdx_cli.main sources kinesis create {HDXCLI_TESTS_DIR}/tests_data/sources/kinesis_source_settings.json test_kinesis_source"]
commands_under_test = ["python3 -m hdx_cli.main sources kinesis delete --disable-confirmation-prompt test_kinesis_source"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Deleted test_kinesis_source'

[[test]]
name = "Kinesis sources can be listed"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources kinesis list"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?test_ci_kinesis_source.*'

[[test]]
name = "Kinesis source settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table --source test_ci_kinesis_source settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result'

# failing because of the pool is crashing
#[[test]]
#name = "Kinesis source name can be modified"
#commands_under_test = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table --source test_ci_kinesis_source settings name new_kinesis_name"]
#teardown = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table --source new_kinesis_name settings name test_ci_kinesis_source"]
#expected_output = 'Updated test_ci_kinesis_source name'

[[test]]
name = "Kinesis source type can be shown"
commands_under_test = ["python3 -m hdx_cli.main sources kinesis --project test_ci_project --table test_ci_table --source test_ci_kinesis_source settings type"]
expected_output = 'type: pull'

[[test]]
name = "Kinesis sources can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources kinesis --source test_ci_kinesis_source show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "\"subtype\": \"kinesis\"" in result'


########################################################## SIEM #########################################################
[[test]]
name = "SIEM sources can be created"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources siem create {HDXCLI_TESTS_DIR}/tests_data/sources/siem_source_settings.json test_siem_source"]
teardown = ["python3 -m hdx_cli.main sources siem delete --disable-confirmation-prompt test_siem_source",
			      "python3 -m hdx_cli.main unset"]
expected_output = 'Created source test_siem_source'

[[test]]
name = "SIEM sources can be deleted"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table",
		     "python3 -m hdx_cli.main sources siem create {HDXCLI_TESTS_DIR}/tests_data/sources/siem_source_settings.json test_siem_source"]
commands_under_test = ["python3 -m hdx_cli.main sources siem delete --disable-confirmation-prompt test_siem_source"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Deleted test_siem_source'

[[test]]
name = "SIEM sources can be listed"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main sources siem list"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?test_ci_siem_source.*'

[[test]]
name = "SIEM source settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main sources siem --project test_ci_project --table test_ci_table --source test_ci_siem_source settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_siem_source" in result'

# failing because of the pool is crashing
#[[test]]
#name = "SIEM source name can be modified"
#commands_under_test = ["python3 -m hdx_cli.main sources siem --project test_ci_project --table test_ci_table --source test_ci_siem_source settings name new_siem_name"]
#teardown = ["python3 -m hdx_cli.main sources --project test_ci_project --table test_ci_table --source new_siem_name siem settings name test_ci_siem_source"]
#expected_output = 'Updated test_ci_siem_source name'
#
#[[test]]
#name = "SIEM source type can be shown"
#commands_under_test = ["python3 -m hdx_cli.main sources siem --project test_ci_project --table test_ci_table --source test_ci_siem_source settings type"]
#expected_output = 'type: pull'
#
#[[test]]
#name = "SIEM sources can be shown"
#setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
#commands_under_test = ["python3 -m hdx_cli.main sources siem --source test_ci_siem_source show"]
#teardown = ["python3 -m hdx_cli.main unset"]
#expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "\"subtype\": \"siem\"" in result'


######################################################## Storage #######################################################
[[test]]
name = "Storages can be created"
commands_under_test = ["python3 -m hdx_cli.main storage create -f {HDXCLI_TESTS_DIR}/tests_data/storages/storage_settings.json test_storage"]
teardown = ["python3 -m hdx_cli.main storage delete --disable-confirmation-prompt test_storage"]
expected_output = 'Created storage test_storage'

[[test]]
name = "Storages can be deleted"
setup = ["python3 -m hdx_cli.main storage create -f {HDXCLI_TESTS_DIR}/tests_data/storages/storage_settings.json test_storage"]
commands_under_test = ["python3 -m hdx_cli.main storage delete --disable-confirmation-prompt test_storage"]
expected_output = 'Deleted test_storage'

[[test]]
name = "Storages can be listed"
commands_under_test = ["python3 -m hdx_cli.main storage list"]
expected_output_re = '.*?test_ci_storage.*'

[[test]]
name = "Storage settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main storage --storage test_ci_storage settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_storage" in result'

[[test]]
name = "Storage cloud can be shown"
commands_under_test = ["python3 -m hdx_cli.main storage --storage test_ci_storage settings settings.cloud"]
expected_output = 'settings.cloud: gcp'

[[test]]
name = "Storage cloud can be modified"
commands_under_test = ["python3 -m hdx_cli.main storage --storage test_ci_storage settings settings.cloud aws"]
teardown = ["python3 -m hdx_cli.main storage --storage test_ci_storage settings settings.cloud gcp"]
expected_output = 'Updated test_ci_storage settings.cloud'

[[test]]
name = "Storages can be shown"
commands_under_test = ["python3 -m hdx_cli.main storage --storage test_ci_storage show"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "bucket_name" in result and "test_ci_storage" in result'


####################################################### Batch Job ######################################################
[[test]]
name = "Batch jobs can be listed"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main job batch list"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_re = '.*?test_ci_batch_job.*'

[[test]]
name = "Batch jobs can be started"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
commands_under_test = ["python3 -m hdx_cli.main job batch --transform test_ci_transform ingest test_batch_job {HDXCLI_TESTS_DIR}/tests_data/batch-jobs/batch_job_settings.json"]
teardown = ["python3 -m hdx_cli.main job batch cancel test_batch_job",
            "python3 -m hdx_cli.main job batch delete --disable-confirmation-prompt test_batch_job",
			      "python3 -m hdx_cli.main unset"]
expected_output = 'Started job test_batch_job'

[[test]]
name = "Batch jobs can be cancelled"
commands_under_test = ["python3 -m hdx_cli.main job batch cancel test_ci_batch_job"]
expected_output = 'Cancelled test_ci_batch_job'

[[test]]
name = "Batch jobs can be retried"
commands_under_test = ["python3 -m hdx_cli.main job batch retry test_ci_batch_job"]
expected_output = 'Retrying test_ci_batch_job'

[[test]]
name = "Batch job settings can be shown"
commands_under_test = ["python3 -m hdx_cli.main job batch --job test_ci_batch_job settings"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_batch_job" in result'

[[test]]
name = "Batch jobs can be shown"
commands_under_test = ["python3 -m hdx_cli.main job batch --job test_ci_batch_job show"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "uuid" in result and "settings" in result and "status" in result and "test_ci_batch_job" in result'

[[test]]
name = "Batch jobs can be deleted"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table",
		     "python3 -m hdx_cli.main job batch --transform test_ci_transform ingest test_batch_job {HDXCLI_TESTS_DIR}/tests_data/batch-jobs/batch_job_settings.json",
		     "python3 -m hdx_cli.main job batch cancel test_batch_job"]
commands_under_test = ["python3 -m hdx_cli.main job batch delete test_batch_job --disable-confirmation-prompt"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Deleted test_batch_job'


######################################################## Stream ########################################################
[[test]]
name = "Stream ingest can be created using CSV file"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table_ingest"]
commands_under_test = ["python3 -m hdx_cli.main stream --transform test_csv_transform ingest {HDXCLI_TESTS_DIR}/tests_data/data/data.csv"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Created stream ingest'

[[test]]
name = "Stream ingest can be created using JSON file"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table_ingest"]
commands_under_test = ["python3 -m hdx_cli.main stream --transform test_json_transform ingest {HDXCLI_TESTS_DIR}/tests_data/data/data.json"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Created stream ingest'

[[test]]
name = "Stream ingest can be created using GZIP compressed file"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table_ingest"]
commands_under_test = ["python3 -m hdx_cli.main stream --transform test_gzip_transform ingest {HDXCLI_TESTS_DIR}/tests_data/data/data.gz"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Created stream ingest'

[[test]]
name = "Stream ingest can be created using ZIP compressed file"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table_ingest"]
commands_under_test = ["python3 -m hdx_cli.main stream --transform test_zip_transform ingest {HDXCLI_TESTS_DIR}/tests_data/data/data.zip"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Created stream ingest'

[[test]]
name = "Stream ingest can be created using ZLIB compressed file"
setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table_ingest"]
commands_under_test = ["python3 -m hdx_cli.main stream --transform test_zlib_transform ingest {HDXCLI_TESTS_DIR}/tests_data/data/data.zlib"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Created stream ingest'


####################################################### Function #######################################################
[[test]]
name = "Functions can be listed"
commands_under_test = ["python3 -m hdx_cli.main function --project test_ci_project list"]
expected_output_re = '.*?test_ci_function.*'

[[test]]
name = "Functions can be created using in-line sql"
commands_under_test = ["python3 -m hdx_cli.main function --project test_ci_project create -s '(x,k,b)->k*x+b' test_function"]
teardown = ["python3 -m hdx_cli.main function --project test_ci_project delete --disable-confirmation-prompt test_function"]
expected_output = 'Created function test_function'

[[test]]
name = "Functions can be created from json file"
commands_under_test = ["python3 -m hdx_cli.main function --project test_ci_project create -f {HDXCLI_TESTS_DIR}/tests_data/functions/function_settings.json test_function"]
teardown = ["python3 -m hdx_cli.main function --project test_ci_project delete --disable-confirmation-prompt test_function"]
expected_output = 'Created function test_function'

[[test]]
name = "Functions can be deleted"
setup = ["python3 -m hdx_cli.main set test_ci_project",
		     "python3 -m hdx_cli.main function create -s '(x,k,b)->k*x+b' test_function"]
commands_under_test = ["python3 -m hdx_cli.main function delete --disable-confirmation-prompt test_function"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output = 'Deleted test_function'

[[test]]
name = "Function settings can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main function --function test_ci_function settings"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_function" in result'

[[test]]
name = "Functions can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main function --function test_ci_function show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "project" in result and "name" in result and "uuid" in result and "sql" in result'


#################################################### Dictionary #######################################################
[[test]]
name = "Dictionaries can be created"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main dictionary create {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_settings.json test_ci_dictionary_file test_dictionary"]
teardown = ["python3 -m hdx_cli.main dictionary delete --disable-confirmation-prompt test_dictionary",
            "python3 -m hdx_cli.main unset"]
expected_output = 'Created test_dictionary'

[[test]]
name = "Dictionaries can be deleted"
setup = ["python3 -m hdx_cli.main dictionary --project test_ci_project create {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_settings.json test_ci_dictionary_file test_dictionary"]
commands_under_test = ["python3 -m hdx_cli.main dictionary --project test_ci_project delete --disable-confirmation-prompt test_dictionary"]
expected_output = 'Deleted test_dictionary'

[[test]]
name = "Dictionaries can be listed"
commands_under_test = ["python3 -m hdx_cli.main dictionary --project test_ci_project list"]
expected_output_re = '.*?test_ci_dictionary.*'

[[test]]
name = "Dictionary settings can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main dictionary --dictionary test_ci_dictionary settings"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "name" in result and "type" in result and "value" in result and "test_ci_dictionary" in result'

[[test]]
name = "Dictionaries can be shown"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main dictionary --dictionary test_ci_dictionary show"]
teardown = ["python3 -m hdx_cli.main unset"]
expected_output_expr = 'not result.startswith("Error:") and "project" in result and "name" in result and "uuid" in result and "filename" in result and "test_ci_dictionary" in result'


################################################### Dictionary Files #####################################################
[[test]]
name = "Dictionary files can be uploaded"
setup = ["python3 -m hdx_cli.main set test_ci_project"]
commands_under_test = ["python3 -m hdx_cli.main dictionary files upload -t verbatim {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_file.csv test_dictionary_file"]
teardown = ["python3 -m hdx_cli.main dictionary files delete test_dictionary_file",
            "python3 -m hdx_cli.main unset"]
expected_output_expr = 'result.startswith("Uploaded dictionary file from") and result.endswith("test_dictionary_file.")'

[[test]]
name = "Dictionary files can be deleted"
setup = ["python3 -m hdx_cli.main dictionary --project test_ci_project files upload -t verbatim {HDXCLI_TESTS_DIR}/tests_data/dictionaries/dictionary_file.csv test_dictionary_file"]
commands_under_test = ["python3 -m hdx_cli.main dictionary --project test_ci_project files delete test_dictionary_file"]
expected_output = 'Deleted test_dictionary_file'

[[test]]
name = "Dictionary files can be listed"
commands_under_test = ["python3 -m hdx_cli.main dictionary --project test_ci_project files list"]
expected_output_re = '.*?test_ci_dictionary_file.*'

######################################################## Role ##########################################################

[[test]]
name = "Create a role with permission"
commands_under_test = ["python -m hdx_cli.main role create --name new_role --permission change_table"]
teardown = ["python -m hdx_cli.main role delete --disable-confirmation-prompt new_role"]
expected_output = 'Created role new_role'

[[test]]
name = "Create existing role"
commands_under_test = ["python -m hdx_cli.main role create --name test_ci_role --permission change_table"]
expected_output = 'Error: This field must be unique.'

[[test]]
name = "Roles can be listed"
commands_under_test = ["python3 -m hdx_cli.main role list"]
expected_output_re = '.*?test_ci_role.*'

[[test]]
name = "add user to a role"
setup = ["python -m hdx_cli.main role create --name new_role --permission change_table"]
commands_under_test = ["python -m hdx_cli.main role add-user --user test_ci_invite_user@hydolix.io new_role"]
teardown = ["python -m hdx_cli.main role delete --disable-confirmation-prompt new_role"]
expected_output = 'Added user(s) to new_role role'

[[test]]
name = "Removed user from role"
setup = ["python -m hdx_cli.main role create --name new_role --permission change_table"]
commands_under_test = ["python3 -m hdx_cli.main role remove-user --user test_ci_invite_user@hydolix.io new_role"]
teardown = ["python -m hdx_cli.main role delete --disable-confirmation-prompt new_role"]
expected_output = 'Removed user(s) from new_role role'

[[test]]
name = "add nonexistent user to a role"
commands_under_test = ["python -m hdx_cli.main role add-user --user nonexistent_user@hydrolix.io test_ci_role"]
expected_output = 'Error: Cannot find users.'

[[test]]
name = "Delete role"
setup = ["python -m hdx_cli.main role create --name new_role --permission change_table"]
commands_under_test = ["python -m hdx_cli.main role delete --disable-confirmation-prompt new_role"]
expected_output_re = 'Deleted new_role'

[[test]]
name = "list permission"
commands_under_test = ["python -m hdx_cli.main role permission list"]
expected_output_re = '.*?view_user.*'

####################################################### Profile ########################################################
##failure in the pipeline due to the error: Error No such file or directory: '/home/runner/.hdx_cli/config.toml'
#[[test]]
#name = "Profiles can be listed"
#commands_under_test = ["python3 -m hdx_cli.main profile list"]
#expected_output_re = '.*?default.*'

##failure in the pipeline due to the error: Error No such file or directory: '/home/runner/.hdx_cli/config.toml'
#[[test]]
#name = "Profiles can be shown"
#setup = ["python3 -m hdx_cli.main --profile default unset"]
#commands_under_test = ["python3 -m hdx_cli.main --profile default profile show"]
#expected_output_expr = '"Profile" in result and "username" in result and "hostname" in result and "projectname" not in result and "tablename" not in result'

## Failing
#[[test]]
#name = "Profile can be shown with preset project/table"
#setup = ["python3 -m hdx_cli.main set test_ci_project test_ci_table"]
#commands_under_test = ["python3 -m hdx_cli.main --profile default profile show"]
#teardown = ["python3 -m hdx_cli.main unset"]
#expected_output_expr = '"Profile" in result and "username" in result and "hostname" in result and "projectname" in result and "tablename" in result'

#profile add -> is there a way to use arguments for cluster, username and scheme?
#profile edit -> is there a way to use arguments for cluster, username and scheme?

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

################################################### query-options ####################################################
[[test]]
name = "Set query options from file"
commands_under_test = ["python3 -m hdx_cli.main query-option set --from-file {HDXCLI_TESTS_DIR}/tests_data/query-options/settings.json"]
teardown = ["python3 -m hdx_cli.main query-option unset --all"]
expected_output_re = '.*?Set query options from file*'

[[test]]
name = "List query options"
setup = ["python3 -m hdx_cli.main query-option set --from-file {HDXCLI_TESTS_DIR}/tests_data/query-options/settings.json"]
commands_under_test = ["python3 -m hdx_cli.main query-option list"]
teardown = ["python3 -m hdx_cli.main query-option unset --all"]
expected_output_re = '.*?hdx_query_max_columns_to_read                          integer        20*'

[[test]]
name = "Unset query options"
setup = ["python3 -m hdx_cli.main query-option set --from-file {HDXCLI_TESTS_DIR}/tests_data/query-options/settings.json"]
commands_under_test = ["python3 -m hdx_cli.main query-option unset --all"]
expected_output = 'Unset all query options'

[[test]]
name = "Set nonexistence query options file"
commands_under_test = ["python3 -m hdx_cli.main query-option set --from-file {HDXCLI_TESTS_DIR}/tests_data/query-options/inexistent_settings.json"]
expected_output_re = 'Error: The specified file does not exist.'

[[test]]
name = "Set query options from name"
commands_under_test = ["python3 -m hdx_cli.main query-option set hdx_query_max_concurrent_partitions 10"]
teardown = ["python3 -m hdx_cli.main query-option unset hdx_query_max_concurrent_partitions"]
expected_output = "Set 'hdx_query_max_concurrent_partitions' query option"

[[test]]
name = "Unset query options from name"
setup = ["python3 -m hdx_cli.main query-option set hdx_query_max_concurrent_partitions 10"]
commands_under_test = ["python3 -m hdx_cli.main query-option unset hdx_query_max_concurrent_partitions"]
expected_output = "Unset 'hdx_query_max_concurrent_partitions' query option"

[[test]]
name = "Set query options nonexistence name"
commands_under_test = ["python3 -m hdx_cli.main query-option set option_nonexistence 10"]
expected_output = "Error: 'option_nonexistence' is not a valid query option."

[[test]]
name = "Unset query options not set from name"
commands_under_test = ["python3 -m hdx_cli.main query-option unset hdx_query_max_concurrent_partitions"]
expected_output = 'Error: hdx_query_max_concurrent_partitions not found in the set query options.'

[[test]]
name = "Set query from name without value"
commands_under_test = ["python3 -m hdx_cli.main query-option set hdx_query_max_concurrent_partitions"]
expected_output = 'Error: You must provide either query_option_name and query_option_value or --from-file (JSON).'

[[test]]
name = "Set query from name with not accepted value"
commands_under_test = ["python3 -m hdx_cli.main query-option set hdx_query_timerange_required 6"]
expected_output = 'Error: Must be a valid boolean.'
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
