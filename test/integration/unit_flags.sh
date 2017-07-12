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

# unit_flags.sh
#
# Collection of unit tests for dsub command-line flags

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running a test of the "--command" flag

function call_dsub() {
  local command="${1:-}"
  local script="${2:-}"

  run_dsub \
    --command "${command}" \
    --script "${script}" \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_missing_command_and_script() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub; then

    2>&1 echo "Neither command nor script specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: One of --command or a script name must be supplied"

    test_passed "${subtest}"
  fi
}
readonly -f test_missing_command_and_script

function test_having_command_and_script() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    'echo "Hello World"' \
    "dummy.sh"; then

    2>&1 echo "Command and script specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Cannot supply both --command and a script name"

    test_passed "${subtest}"
  fi
}
readonly -f test_having_command_and_script

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_missing_command_and_script
test_having_command_and_script
