#!/bin/bash

# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset

# unit_io_tasks.google.sh
#
# Simple unit tests of input and output tasks arguments

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the tests

function call_dsub() {
  local tasks="${1}"
  local vars_include_wildcards="${2:-}"

  run_dsub \
    --script "${SCRIPT}" \
    --tasks "${tasks}" \
    ${vars_include_wildcards:+--vars-include-wildcards} \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

function check_deprecation_warning() {
  local expected_var="${1}"
  assert_err_contains \
    "\
WARNING: The behavior of docker environment variables for input
         parameters with wildcard (*) values is changing.
         Set --vars-include-wildcards to enable the new behavior so
         you can change your code before it becomes the default.
         The following input parameters include wildcards:
           ${expected_var}"

  # Remove the deprecation warning from stderr for downstream output checks
  sed -i '1,6 d' "${TEST_STDERR}"
}
readonly -f check_deprecation_warning

# Define tests

function test_input_file() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--input INPUT_PATH1\t--input INPUT_PATH2
gs://bucket1/path1\tgs://bucket1/path2
gs://bucket2/path1\tgs://bucket2/path2
'

  if call_dsub "${tsv_file}"; then

    # Check that the output contains expected paths

    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH1" "input/gs/bucket1/path1" "gs://bucket1/path1"

    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH2" "input/gs/bucket1/path2" "gs://bucket1/path2"

    assert_pipeline_input_parameter_equals \
      1 "INPUT_PATH1" "input/gs/bucket2/path1" "gs://bucket2/path1"

    assert_pipeline_input_parameter_equals \
      1 "INPUT_PATH2" "input/gs/bucket2/path2" "gs://bucket2/path2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_file

function test_input_auto() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--input\t--input
gs://bucket1/path1\tgs://bucket1/path2
gs://bucket2/path1\tgs://bucket2/path2
'

  if call_dsub "${tsv_file}"; then

    # Check that the output contains expected paths

    assert_pipeline_input_parameter_equals \
      0 "INPUT_0" "input/gs/bucket1/path1" "gs://bucket1/path1"

    assert_pipeline_input_parameter_equals \
      0 "INPUT_1" "input/gs/bucket1/path2" "gs://bucket1/path2"

    assert_pipeline_input_parameter_equals \
      1 "INPUT_0" "input/gs/bucket2/path1" "gs://bucket2/path1"

    assert_pipeline_input_parameter_equals \
      1 "INPUT_1" "input/gs/bucket2/path2" "gs://bucket2/path2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_auto

function test_output_file() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--output OUTPUT_PATH1\t--output OUTPUT_PATH2
gs://bucket1/path1\tgs://bucket1/path2
gs://bucket2/path1\tgs://bucket2/path2
'

  if call_dsub "${tsv_file}"; then

    # Check that the output contains expected paths

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_PATH1" "output/gs/bucket1/path1" "gs://bucket1/path1"

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_PATH2" "output/gs/bucket1/path2" "gs://bucket1/path2"

    assert_pipeline_output_parameter_equals \
      1 "OUTPUT_PATH1" "output/gs/bucket2/path1" "gs://bucket2/path1"

    assert_pipeline_output_parameter_equals \
      1 "OUTPUT_PATH2" "output/gs/bucket2/path2" "gs://bucket2/path2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_file

function test_output_auto() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--output\t--output
gs://bucket1/path1\tgs://bucket1/path2
gs://bucket2/path1\tgs://bucket2/path2
'

  if call_dsub "${tsv_file}"; then

    # Check that the output contains expected paths

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_0" "output/gs/bucket1/path1" "gs://bucket1/path1"

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_1" "output/gs/bucket1/path2" "gs://bucket1/path2"

    assert_pipeline_output_parameter_equals \
      1 "OUTPUT_0" "output/gs/bucket2/path1" "gs://bucket2/path1"

    assert_pipeline_output_parameter_equals \
      1 "OUTPUT_1" "output/gs/bucket2/path2" "gs://bucket2/path2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_auto


function test_input_wildcard_deprecated() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--input-recursive INPUT_PATH_DEEP\t--input INPUT_PATH_SHALLOW
gs://bucket1/path1/deep\tgs://bucket1/path1/shallow/*
'

  if call_dsub "${tsv_file}"; then
    check_deprecation_warning \
      "INPUT_PATH_SHALLOW=gs://bucket1/path1/shallow/*"

    # Ensure the INPUT_PATH_SHALLOW parameter is set properly
    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH_SHALLOW" \
      "input/gs/bucket1/path1/shallow/" "gs://bucket1/path1/shallow/*"

    # Ensure the docker command does not include setting INPUT_PATH_SHALLOW
    assert_err_not_contains \
      "INPUT_PATH_SHALLOW="

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_wildcard_deprecated

function test_input_wildcard_and_recursive() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--input-recursive INPUT_PATH_DEEP\t--input INPUT_PATH_SHALLOW
gs://bucket1/path1/deep\tgs://bucket1/path1/shallow/*
'

  if call_dsub "${tsv_file}" "true"; then

    # A direct export of an environment variable for INPUT_PATH_DEEP
    # should be created in the docker command instead of a pipelines
    # output parameter.

    # No INPUT_PATH_DEEP output parameter should have been created
    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH_DEEP" "" ""

    # Ensure the INPUT_PATH_SHALLOW parameter is set properly
    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH_SHALLOW" \
      "input/gs/bucket1/path1/shallow/" "gs://bucket1/path1/shallow/*"

    # The docker command should include an export of INPUT_PATH_SHALLOW
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      '^export INPUT_PATH_SHALLOW="/mnt/data/input/gs/bucket1/path1/shallow/\*"$'

    # The docker command should include an export of OUTPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "^export INPUT_PATH_DEEP=/mnt/data/input/gs/bucket1/path1/deep$"

    # The docker command should include an rsync of INPUT_PATH_DEEP
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "gsutil -m rsync -r gs://bucket1/path1/deep/ /mnt/data/input/gs/bucket1/path1/deep/"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_wildcard_and_recursive

function test_output_wildcard_and_recursive() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--output-recursive OUTPUT_PATH_DEEP\t--output OUTPUT_PATH_SHALLOW
gs://bucket1/path1/deep\tgs://bucket1/path1/shallow/*
'

  if call_dsub "${tsv_file}"; then

    # A direct export of an environment variable for OUTPUT_PATH_DEEP
    # should be created in the docker command instead of a pipelines
    # output parameter.

    # No OUTPUT_PATH_DEEP output parameter should have been created
    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_PATH_DEEP" "" ""

    # Ensure the OUTPUT_PATH_SHALLOW parameter is set properly
    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_PATH_SHALLOW" \
      "output/gs/bucket1/path1/shallow/*" "gs://bucket1/path1/shallow/"

    # The docker command should include an export of the OUTPUT_PATH_DEEP
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "^export OUTPUT_PATH_DEEP=/mnt/data/output/gs/bucket1/path1/deep$"

    # The docker command should include an rsync of the OUTPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "gsutil -m rsync -r /mnt/data/output/gs/bucket1/path1/deep/ gs://bucket1/path1/deep/"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_wildcard_and_recursive

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_input_file
test_input_auto

echo
test_output_file
test_output_auto

echo
test_input_wildcard_deprecated
test_input_wildcard_and_recursive
test_output_wildcard_and_recursive
