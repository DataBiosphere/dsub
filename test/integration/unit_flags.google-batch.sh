#!/bin/bash

# Copyright 2024 Verily Life Sciences Inc. All Rights Reserved.
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

# unit_flags.google-batch.sh
#
# Collection of unit tests for dsub command-line flags
# specific to the google-batch provider.

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

function test_boot_disk_size() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --boot-disk-size '50'; then

    # Check that the output contains expected values
    result=$(grep -A 2 boot_disk "${TEST_STDERR}" | grep size_gb | awk '{print $2}')
    if [[ "${result}" != "50" ]]; then
        1>&2 echo "boot-disk-size was actually ${result}, expected 50"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_boot_disk_size

function test_accelerator_type_and_count() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --accelerator-type "nvidia-tesla-k80" \
    --accelerator-count 2; then

    # Check that the output contains expected values
    type_result=$(grep -A 2 accelerators "${TEST_STDERR}" | grep type_ | awk -F\" '{print $2}')
    if [[ "${type_result}" != "nvidia-tesla-k80" ]]; then
        1>&2 echo "accelerator type was actually ${type_result}, expected nvidia-tesla-k80"
        exit 1
    fi
    count_result=$(grep -A 2 accelerators "${TEST_STDERR}" | grep count | awk '{print $2}')
    if [[ "${count_result}" != "2" ]]; then
        1>&2 echo "accelerator count was actually ${count_result}, expected 2"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_accelerator_type_and_count

function test_no_accelerator_type_and_count() {
  local subtest="${FUNCNAME[0]}"
  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    if grep -A 2 accelerators "${TEST_STDERR}"; then
        1>&2 echo "accelerators unexpectedly in request when it shouldn't be"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_accelerator_type_and_count

function test_machine_type() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --machine-type "n1-highmem-2"; then

    # Check that the output contains expected values
    type_result=$(grep machine_type "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${type_result}" != "n1-highmem-2" ]]; then
        1>&2 echo "accelerator type was actually ${type_result}, expected n1-highmem-2"
        exit 1
    fi

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
    --machine-type "n1-standard-1"; then

    # Check that the output contains expected values
    type_result=$(grep machine_type "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${type_result}" != "n1-standard-1" ]]; then
        1>&2 echo "accelerator type was actually ${type_result}, expected n1-highmem-2"
        exit 1
    fi

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_machine_type

function test_min_cores() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --min-cores 1; then

    # Check that the output contains expected values
    type_result=$(grep machine_type "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${type_result}" != "custom-1-3840" ]]; then
        1>&2 echo "accelerator type was actually ${type_result}, expected custom-1-3840"
        exit 1
    fi
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
    --min-ram 1; then

    # Check that the output contains expected values
    type_result=$(grep machine_type "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${type_result}" != "custom-1-1024" ]]; then
        1>&2 echo "accelerator type was actually ${type_result}, expected custom-1-1024"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_min_ram

function test_service_account() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --service-account 'foo@bar.com'; then

    # Check that the output contains expected values
    result=$(grep -A 2 service_account "${TEST_STDERR}" | grep email | awk -F\" '{print $2}')
    if [[ "${result}" != "foo@bar.com" ]]; then
        1>&2 echo "service account was actually ${result}, expected foo@bar.com"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_service_account

function test_no_service_account() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    if grep -A 2 service_account "${TEST_STDERR}"; then
        1>&2 echo "service_account unexpectedly in request when it shouldn't be"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_no_service_account

function test_network() {
  local subtest="${FUNCNAME[0]}"

  if DOCKER_IMAGE_OVERRIDE="marketplace.gcr.io/google/debian9" call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --network 'network-name-foo' \
    --subnetwork 'subnetwork-name-foo' \
    --use-private-address; then

    # Check that the output contains expected values
    network_result=$(grep " network:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${network_result}" != "network-name-foo" ]]; then
        1>&2 echo "network was actually ${network_result}, expected network-name-foo"
        exit 1
    fi
    subnetwork_result=$(grep "subnetwork:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${subnetwork_result}" != "subnetwork-name-foo" ]]; then
        1>&2 echo "subnetwork was actually ${subnetwork_result}, expected subnetwork-name-foo"
        exit 1
    fi
    private_result=$(grep "no_external_ip_address:" "${TEST_STDERR}" | awk '{print $2}')
    if [[ "${private_result}" != "true" ]]; then
        1>&2 echo "no_external_ip_address was actually ${private_result}, expected true"
        exit 1
    fi

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_network

function test_location() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --location us-west2 \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    location_result=$(grep " allowed_locations:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${location_result}" != "regions/us-west2" ]]; then
        1>&2 echo "location was actually ${location_result}, expected regions/us-west2"
        exit 1
    fi

    test_passed "${subtest}"
  else
    1>&2 echo "Using the location flag generated an error"

    test_failed "${subtest}"
  fi
}
readonly -f test_location

function test_neither_region_nor_zone() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"'; then

    # Check that the output contains expected values
    location_result=$(grep " allowed_locations:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${location_result}" != "regions/us-central1" ]]; then
        1>&2 echo "location was actually ${location_result}, expected regions/us-central1"
        exit 1
    fi

    test_passed "${subtest}"
  else
    1>&2 echo "Location not used as default region"

    test_failed "${subtest}"
  fi
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

    assert_err_contains \
      "ValueError: At most one of --regions and --zones may be specified"

    test_passed "${subtest}"
  fi
}
readonly -f test_neither_region_nor_zone

function test_regions() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --regions us-central1; then

    # Check that the output contains expected values
    location_result=$(grep " allowed_locations:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${location_result}" != "regions/us-central1" ]]; then
        1>&2 echo "location was actually ${location_result}, expected regions/us-central1"
        exit 1
    fi

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
    location_result=$(grep " allowed_locations:" "${TEST_STDERR}" | awk -F\" '{print $2}')
    if [[ "${location_result}" != "zones/us-central1-a" ]]; then
        1>&2 echo "location was actually ${location_result}, expected zones/us-central1-a"
        exit 1
    fi

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_zones

function test_preemptible_zero() {
  local subtest="${FUNCNAME[0]}"

  if call_dsub \
    --command 'echo "${TEST_NAME}"' \
    --preemptible 0; then

    # Check that the output contains expected values
    result=$(grep " provisioning_model:" "${TEST_STDERR}" | awk '{print $2}')
    if [[ "${result}" != "STANDARD" ]]; then
        1>&2 echo "provisioning_model was actually ${result}, expected STANDARD"
        exit 1
    fi
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
    result=$(grep " provisioning_model:" "${TEST_STDERR}" | awk '{print $2}')
    if [[ "${result}" != "STANDARD" ]]; then
        1>&2 echo "provisioning_model was actually ${result}, expected STANDARD"
        exit 1
    fi
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
    result=$(grep " provisioning_model:" "${TEST_STDERR}" | awk '{print $2}')
    if [[ "${result}" != "SPOT" ]]; then
        1>&2 echo "provisioning_model was actually ${result}, expected SPOT"
        exit 1
    fi
    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_preemptible_on

# # Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_boot_disk_size

echo
test_accelerator_type_and_count
test_no_accelerator_type_and_count

echo
test_machine_type
test_no_machine_type

echo
test_min_cores
test_min_ram

echo
test_service_account
test_no_service_account

echo
test_network

echo
test_location
test_neither_region_nor_zone
test_region_and_zone
test_regions
test_zones

echo
test_preemptible_zero
test_preemptible_off
test_preemptible_on
