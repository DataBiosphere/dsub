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

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.machineType" "custom-1-3840"
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_min_cores

function test_min_ram() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --min-ram 1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.machineType" "custom-1-1024"
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
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

function test_machine_type_with_ram_and_cpu() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --machine-type "n1-highmem-2" \
    --min-cores 1 \
    --min-ram 1; then

    2>&1 echo "min-ram/min-cores set with machine-type on google-v2 provider - not detected"

    test_failed "${subtest}"
  else
    assert_output_empty

    assert_err_contains \
      "ValueError: --machine-type not supported together with --min-cores or --min-ram."

    test_passed "${subtest}"
  fi
}
readonly -f test_machine_type_with_ram_and_cpu

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

function test_network() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --network 'network-name-foo' \
    --subnetwork 'subnetwork-name-foo' \
    --use-private-address; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.name" "network-name-foo"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.subnetwork" "subnetwork-name-foo"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.usePrivateAddress" "True"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_network

function test_no_network() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.name" "None"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.subnetwork" "None"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.usePrivateAddress" "False"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_network

function test_cpu_platform() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --cpu-platform 'Intel Skylake'; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.cpuPlatform" "Intel Skylake"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_cpu_platform

function test_no_cpu_platform() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.cpuPlatform" "None"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_cpu_platform

function test_timeout() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --timeout '1h'; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.timeout" "3600.0s"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_timeout

function test_no_timeout() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.timeout" "None"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_timeout

function test_log_interval() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --log-interval '1h'; then

    # Check that the output contains expected values
    assert_err_value_matches \
     "[0].pipeline.actions.[0].commands.[1]" "3600.0s"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_log_interval

function test_no_log_interval() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_matches \
     "[0].pipeline.actions.[0].commands.[1]" "60s"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_log_interval

function test_ssh() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --ssh; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.actions.[1].entrypoint" "ssh-server"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_ssh

function test_no_ssh() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output does not contain ssh values
    assert_err_not_contains \
     "ssh-server"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_ssh

function test_user_project() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --user-project 'sample-project-name'; then

    # Check for the USER_PROJECT in the environment for the
    # logging, localization, delocalization, and final_logging actions
    assert_err_value_matches \
     "[0].pipeline.actions.[0].environment.USER_PROJECT" "sample-project-name"
    assert_err_value_matches \
     "[0].pipeline.actions.[2].environment.USER_PROJECT" "sample-project-name"
    assert_err_value_matches \
     "[0].pipeline.actions.[4].environment.USER_PROJECT" "sample-project-name"
    assert_err_value_matches \
     "[0].pipeline.actions.[5].environment.USER_PROJECT" "sample-project-name"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_user_project

function test_no_user_project() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_not_contains "sample-project-name"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_user_project

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
test_machine_type_with_ram_and_cpu

echo
test_accelerator_type_and_count
test_no_accelerator_type_and_count

echo
test_network
test_no_network

echo
test_cpu_platform
test_no_cpu_platform

echo
test_timeout
test_no_timeout

echo
test_log_interval
test_no_log_interval

echo
test_ssh
test_no_ssh

echo
test_user_project
test_no_user_project
