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
  dsub \
    --provider google-v2 \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    "${@}" \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

# Define tests

function test_neither_region_nor_zone() {
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
readonly -f test_neither_region_nor_zone

function test_region_and_zone() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --zones us-central1-f \
    --regions us-central1; then

    2>&1 echo "Both regions and zones specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Exactly one of --regions and --zones must be specified"

    test_passed "${subtest}"
  fi
}
readonly -f test_region_and_zone

function test_min_cores() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --min-cores 1; then

    2>&1 echo "min-cores set with google-v2 provider - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Not supported with the google-v2 provider: --min-cores. Use --machine-type instead."

    test_passed "${subtest}"
  fi
}
readonly -f test_min_cores

function test_min_ram() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --min-ram 1; then

    2>&1 echo "min-ram set with google-v2 provider - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Not supported with the google-v2 provider: --min-ram. Use --machine-type instead."

    test_passed "${subtest}"
  fi
}
readonly -f test_min_ram

function test_machine_type() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --machine-type "n1-highmem-2"; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.machineType" "n1-highmem-2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_machine_type

function test_no_machine_type() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.machineType" "n1-standard-1"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_machine_type

function test_accelerator_type_and_count() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --accelerator-type "nvidia-tesla-k80" \
    --accelerator-count 2; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.accelerators.[0].type" "nvidia-tesla-k80"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.accelerators.[0].count" "2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_accelerator_type_and_count

function test_no_accelerator_type_and_count() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.accelerators" "None"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_accelerator_type_and_count

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_neither_region_nor_zone
test_region_and_zone

echo
test_min_cores
test_min_ram
test_machine_type
test_no_machine_type

echo
test_accelerator_type_and_count
test_no_accelerator_type_and_count
