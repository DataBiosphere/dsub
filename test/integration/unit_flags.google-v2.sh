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

# unit_flags.google-v2.sh
#
# Collection of unit tests for dsub command-line flags
# specific to the google-v2 provider.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

function call_dsub() {
  run_dsub \
    "${@}" \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_with_neither_region_nor_zone() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    2>&1 echo "Neither regions nor zones specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Exactly one of --regions and --zones must be specified"

    test_passed "${subtest}"
  fi
}
readonly -f test_with_neither_region_nor_zone

function test_with_region_and_zone() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --zones us-central1-f \
    --regions us-central1; then

    2>&1 echo "Neither regions nor zones specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Exactly one of --regions and --zones must be specified"

    test_passed "${subtest}"
  fi
}
readonly -f test_with_region_and_zone

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_with_neither_region_nor_zone
test_with_region_and_zone
