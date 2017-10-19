#!/bin/bash

# Copyright 2017 Google Inc. All Rights Reserved.
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

# unit_tasks.sh
#
# Simple unit tests of the --tasks argument.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the IO test

function call_dsub() {
  run_dsub \
    "$@" \
    --command 'echo hello' \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_task_max() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" "-3"; then
    2>&1 echo "Invalid tasks lines range specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Task range minimum must be set"

    test_passed "${subtest}"
  fi
}
readonly -f test_task_max

function test_duplicate_label() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" \
    --label "my-label=banana"; then

    2>&1 echo "Duplicate label specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Names for labels on the command-line and in the --tasks file must not be repeated: my-label"

    test_passed "${subtest}"
  fi
}
readonly -f test_duplicate_label

function test_duplicate_env() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" \
    --env "MY_ENV=value"; then

    2>&1 echo "Duplicate env specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Names for envs, inputs, and outputs on the command-line and in the --tasks file must not be repeated: MY_ENV"

    test_passed "${subtest}"
  fi
}
readonly -f test_duplicate_env

function test_duplicate_input() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" \
    --input "INPUT_PATH=value"; then

    2>&1 echo "Duplicate input specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Names for envs, inputs, and outputs on the command-line and in the --tasks file must not be repeated: INPUT_PATH"

    test_passed "${subtest}"
  fi
}
readonly -f test_duplicate_input

function test_duplicate_output() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" \
    --output "OUTPUT_PATH=value"; then

    2>&1 echo "Duplicate output specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Names for envs, inputs, and outputs on the command-line and in the --tasks file must not be repeated: OUTPUT_PATH"

    test_passed "${subtest}"
  fi
}
readonly -f test_duplicate_output

function test_duplicate_env_and_input() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --tasks "${TSV_FILE}" \
    --env "INPUT_PATH=value"; then

    2>&1 echo "Duplicate output specified, but not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: Names for envs, inputs, and outputs on the command-line and in the --tasks file must not be repeated: INPUT_PATH"

    test_passed "${subtest}"
  fi
}
readonly -f test_duplicate_env_and_input

# Set up for running the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

# Create a simple TSV file
readonly TSV_FILE="${TEST_TMP}/${TEST_NAME}.tsv"
util::write_tsv_file "${TSV_FILE}" \
'
  --label my-label\t--env MY_ENV\t--input INPUT_PATH\t--output OUTPUT_PATH
  item-1\tenv-1\tgs://bucket1/input_path\tgs://bucket1/output_path
  item-2\tenv-2\tgs://bucket2/input_path\tgs://bucket2/output_path
  item-3\tenv-3\tgs://bucket3/input_path\tgs://bucket3/output_path
  item-4\tenv-4\tgs://bucket4/input_path\tgs://bucket4/output_path
  item-5\tenv-5\tgs://bucket5/input_path\tgs://bucket5/output_path
'

# Run the tests
echo
test_task_max

test_duplicate_label
test_duplicate_env
test_duplicate_input
test_duplicate_output
test_duplicate_env_and_input
