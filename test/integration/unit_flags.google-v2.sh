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
  local image="${DOCKER_IMAGE_OVERRIDE:-dummy-image}"
  
  dsub \
    --provider "${DSUB_PROVIDER}" \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    --image "${image}" \
    "${@}" \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f call_dsub

if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]]; then
  readonly NETWORK_NAME_KEY="network"
elif [[ "${DSUB_PROVIDER}" == "google-v2" ]]; then
  readonly NETWORK_NAME_KEY="name"
fi

# Define tests

function test_preemptible_zero() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --preemptible 0; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.preemptible" "False"
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_preemptible_zero

function test_preemptible_off() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.preemptible" "False"
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_preemptible_off

function test_preemptible_on() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --preemptible; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.preemptible" "True"
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_preemptible_on

# A google-cls-v2 test that the location value is settable and used
# for the region.
function test_location() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --location us-west2 \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.regions.[0]" "us-west2"
    assert_err_value_equals \
     "[0].pipeline.resources.zones" "[]"

    test_passed "${subtest}"
  else
    1>&2 echo "Using the location flag generated an error"

    test_failed "${subtest}"
  fi
}
readonly -f test_location

function test_neither_region_nor_zone_google-cls-v2() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.regions.[0]" "us-central1"
    assert_err_value_equals \
     "[0].pipeline.resources.zones" "[]"

    test_passed "${subtest}"
  else
    1>&2 echo "Location not used as default region"

    test_failed "${subtest}"
  fi
}
readonly -f test_neither_region_nor_zone_google-cls-v2

function test_neither_region_nor_zone_google-v2() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    1>&2 echo "Neither regions nor zones specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: Exactly one of --regions and --zones must be specified"

    test_passed "${subtest}"
  fi
}
readonly -f test_neither_region_nor_zone_google-v2

function test_neither_region_nor_zone() {
  test_neither_region_nor_zone_"${DSUB_PROVIDER}"
}
readonly -f test_neither_region_nor_zone

function test_region_and_zone() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --zones us-central1-f \
    --regions us-central1; then

    1>&2 echo "Both regions and zones specified - not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]]; then
      assert_err_contains \
        "ValueError: At most one of --regions and --zones may be specified"
    elif [[ "${DSUB_PROVIDER}" == "google-v2" ]]; then
      assert_err_contains \
        "ValueError: Exactly one of --regions and --zones must be specified"
    fi

    test_passed "${subtest}"
  fi
}
readonly -f test_region_and_zone

function test_regions() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.regions.[0]" "us-central1"
    assert_err_value_equals \
     "[0].pipeline.resources.zones" "[]"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_regions

function test_zones() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --zones us-central1-a; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.regions" "[]"
    assert_err_value_equals \
     "[0].pipeline.resources.zones.[0]" "us-central1-a"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_zones

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

    1>&2 echo "min-ram/min-cores set with machine-type on google-v2 provider - not detected"

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
    --nvidia-driver-version "390.46" \
    --accelerator-count 2; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.accelerators.[0].type" "nvidia-tesla-k80"
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.nvidiaDriverVersion" "390.46"
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
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.nvidiaDriverVersion" "None"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_accelerator_type_and_count

function test_network() {
  local subtest="${FUNCNAME[0]}"

  if DOCKER_IMAGE_OVERRIDE="marketplace.gcr.io/google/debian9" call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --network 'network-name-foo' \
    --subnetwork 'subnetwork-name-foo' \
    --use-private-address; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.network.${NETWORK_NAME_KEY}" "network-name-foo"
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
     "[0].pipeline.resources.virtualMachine.network.${NETWORK_NAME_KEY}" "None"
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

function test_use_private_address_with_public_image() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --use-private-address; then

    1>&2 echo "Public image used with no public address was not detected"

    test_failed "${subtest}"
  else

    assert_output_empty

    assert_err_contains \
      "ValueError: --use-private-address must specify a --image with a gcr.io host"

    test_passed "${subtest}"
  fi
}
readonly -f test_use_private_address_with_public_image

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

function test_service_account() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --service-account 'foo@bar.com'; then

    # Check for the service account email
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.serviceAccount.email" "foo@bar.com"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_service_account

function test_no_service_account() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
     assert_err_value_equals \
      "[0].pipeline.resources.virtualMachine.serviceAccount.email" "default"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_service_account

function test_disk_type() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --disk-type 'pd-ssd'; then

    # Check that the output contains expected values
    assert_err_value_equals \
      "[0].pipeline.resources.virtualMachine.disks.[0].type" "pd-ssd"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_disk_type

function test_no_disk_type() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 ; then

    # Check that the output contains expected values
    assert_err_value_equals \
      "[0].pipeline.resources.virtualMachine.disks.[0].type" "pd-standard"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_disk_type

function test_stackdriver() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1 \
    --enable-stackdriver-monitoring; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.enableStackdriverMonitoring" "True"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_stackdriver

function test_no_stackdriver() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    assert_err_value_equals \
     "[0].pipeline.resources.virtualMachine.enableStackdriverMonitoring" "False"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_stackdriver


# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]]; then
  test_location
fi

echo
test_preemptible_zero
test_preemptible_off
test_preemptible_on

echo
test_neither_region_nor_zone
test_region_and_zone
test_regions
test_zones

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
test_use_private_address_with_public_image

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

echo
test_service_account
test_no_service_account

echo
test_disk_type
test_no_disk_type

echo
test_stackdriver
test_no_stackdriver
