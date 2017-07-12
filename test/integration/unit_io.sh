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

# unit_io.sh
#
# Simple unit tests of the --input and --output arguments.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the IO test

function call_dsub() {
  local input="${1}"
  local output="${2}"

  if [[ -z "${input}" ]]; then
    unset input
  fi
  if [[ -z "${output}" ]]; then
    unset output
  fi

  run_dsub \
    --script "${SCRIPT}" \
    --env TEST_NAME="${TEST_NAME}" \
    ${input:+--input "${input}"} \
    ${output:+--output "${output}"} \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_input_bad_wildcard() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
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

  if call_dsub \
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

function test_output_bad_path() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
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

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_input_bad_wildcard
test_input_bad_recursive_wildcard

echo
test_output_bad_path
