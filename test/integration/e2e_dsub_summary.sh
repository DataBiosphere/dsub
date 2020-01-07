#!/bin/bash

# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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

# This test launches a job with multiple tasks and then verifies
# that summary output exists with the --summary flag.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly TEST_STDOUT=${TEST_TMP}/stdout.txt
readonly TEST_STDERR=${TEST_TMP}/stderr.txt

function assert_err_contains() {
  local expected="${1}"

  if ! grep --quiet --fixed-strings "${expected}" "${TEST_STDERR}"; then
    1>&2 echo "Assert: stderr does not contain expected output:"
    1>&2 echo "EXPECTED:"
    1>&2 echo "${expected}"
    1>&2 echo "ACTUAL:"
    1>&2 echo "$(<$"${TEST_STDERR}")"

    exit 1
  fi
}
readonly -f assert_err_contains

function test_passed() {
  local test_name="${1}"
  echo "Test ${test_name}: PASSED"
}
readonly -f test_passed

function test_failed() {
  local test_name="${1}"
  echo "Test ${test_name}: FAILED"

  exit 1
}
readonly -f test_failed

function call_dsub() {
  run_dsub \
    --summary \
    --wait \
    "${@}" \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub


function test_summary() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    assert_err_contains \
      "Job Name    Status      Task Count"
    test_passed "${subtest}"
  else
    echo "stdout:"
    cat "${TEST_STDOUT}"
    echo "stderr:"
    cat "${TEST_STDERR}"
    test_failed "${subtest}"
  fi
}
readonly -f test_summary

function test_summary_with_retries() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --retries 1; then

    # Check that the output contains expected values
    assert_err_contains \
      "Job Name    Status      Task Count"
    test_passed "${subtest}"
  else
    echo "stdout:"
    cat "${TEST_STDOUT}"
    echo "stderr:"
    cat "${TEST_STDERR}"
    test_failed "${subtest}"
  fi
}
readonly -f test_summary_with_retries

mkdir -p "${TEST_TMP}"

echo
test_summary
test_summary_with_retries
