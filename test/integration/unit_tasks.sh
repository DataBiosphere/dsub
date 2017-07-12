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
  local task_path="${1}"
  local task_range="${2:-}"

  run_dsub \
    --tasks "${task_path}" "${task_range}" \
    --command 'echo hello' \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_task_max() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub "${TSV_FILE}" "-3"; then
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

# Set up for running the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

# Create a simple TSV file
readonly TSV_FILE="${TEST_TMP}/${TEST_NAME}.tsv"
util::write_tsv_file "${TSV_FILE}" \
'
  --input INPUT_PATH
  gs://bucket1/path
  gs://bucket2/path
  gs://bucket3/path
  gs://bucket4/path
  gs://bucket5/path
'

# Run the tests
echo
test_task_max
