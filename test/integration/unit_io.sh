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

# unit_test_io.sh
#
# Simple unit tests of the --input and --output arguments.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the IO test

function run_dsub() {
  local input="${1}"
  local output="${2}"
  local input_recursive="${3:-}"
  local output_recursive="${4:-}"

  if [[ -z "${input}" ]]; then
    unset input
  fi
  if [[ -z "${output}" ]]; then
    unset output
  fi

  "${DSUB}" \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "${ZONE}" \
    --script "${SCRIPT}" \
    --env TEST_NAME="${TEST_NAME}" \
    ${input:+--input "${input}"} \
    ${output:+--output "${output}"} \
    ${input_recursive:+--input-recursive "${input_recursive}"} \
    ${output_recursive:+--output-recursive "${output_recursive}"} \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f run_dsub

# Define tests

function test_input_file() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file.bam \
    gs://bucket/path/*; then

    # Check that the output contains expected paths

    assert_pipeline_input_parameter_equals \
      0 "INPUT_0" "input/gs/bucket/path/file.bam" "gs://bucket/path/file.bam"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_file

function test_input_wildcard() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file*.bam \
    gs://bucket/path/*; then

    # Check that the output contains expected paths

    assert_pipeline_input_parameter_equals \
      0 "INPUT_0" "input/gs/bucket/path/" "gs://bucket/path/file*.bam"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_wildcard

function test_input_bad_wildcard() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/*/file.bam \
    gs://bucket/path/*; then

    2>&1 echo "Wildcard error on input not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Wildcards in remote paths only supported for files: gs://bucket/*/file.bam"

    test_passed "${subtest}"
  fi
}
readonly -f test_input_bad_wildcard

function test_input_bad_recursive_wildcard() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file**.bam \
    gs://bucket/path/*; then

    2>&1 echo "Recursive wildcard error on input not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Recursive wildcards (\"**\") not supported: gs://bucket/path/file**.bam"

    test_passed "${subtest}"
  fi
}
readonly -f test_input_bad_recursive_wildcard

function test_output_file() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file.bam \
    gs://bucket/path/file.bam.bai; then

    # Test that output includes creation of output directories
    assert_err_contains "mkdir -p /mnt/data/output/gs/bucket/path"

    # Test that output contains expected paths

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_0" \
      "output/gs/bucket/path/file.bam.bai" "gs://bucket/path/file.bam.bai"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_file

function test_output_wildcard() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file.bam \
    gs://bucket/path/*.bai; then

    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_0" "output/gs/bucket/path/*.bai" "gs://bucket/path/"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_wildcard

function test_output_bad_path() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    gs://bucket/path/file.bam \
    gs://bucket/path/; then

    2>&1 echo "Output path error not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Output variables that are not recursive must reference a filename or wildcard: gs://bucket/path/"

    test_passed "${subtest}"
  fi
}
readonly -f test_output_bad_path

function test_input_recursive() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    "" \
    "" \
    "INPUT_PATH=gs://bucket/path/" \
    ""; then

    # No INPUT_PATH input parameter should have been created
    assert_pipeline_input_parameter_equals \
      0 "INPUT_PATH" "" ""

    # The docker command should include an export of the INPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "^export INPUT_PATH=/mnt/data/input/gs/bucket/path$"

    # The docker command should include an rsync of the OUTPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "gsutil -m rsync -r gs://bucket/path/ /mnt/data/input/gs/bucket/path/"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_input_recursive

function test_output_recursive() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub \
    "" \
    "" \
    "" \
    "OUTPUT_PATH=gs://bucket/path/"; then

    # No OUTPUT_PATH output parameter should have been created
    assert_pipeline_output_parameter_equals \
      0 "OUTPUT_PATH" "" ""

    # The docker command should include an export of the OUTPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "^export OUTPUT_PATH=/mnt/data/output/gs/bucket/path$"

    # The docker command should include an rsync of the OUTPUT_PATH
    assert_err_value_matches \
      "[0].ephemeralPipeline.docker.cmd" \
      "gsutil -m rsync -r /mnt/data/output/gs/bucket/path/ gs://bucket/path/"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_output_recursive


# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TEMP}"

echo
test_input_file
test_input_wildcard

test_input_bad_wildcard
test_input_bad_recursive_wildcard

echo
test_output_file
test_output_bad_path
test_output_wildcard

echo
test_input_recursive
test_output_recursive
