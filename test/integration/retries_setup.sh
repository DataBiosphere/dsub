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

# Setup file to be sourced into e2e_retries*.sh tests.

function retries_setup::get_job_name() {
  local script_name="${1}"

  # Get the job name from the script name,
  # Trim the "e2e_" prefix and ".sh" suffix.
  # Replace underscores with hyphens as GCP labels do not support underscores.
  echo "${script_name}" | sed -e 's#^e2e_##' -e 's#.sh$##' -e 's/_/-/g'
}
readonly -f retries_setup::get_job_name

function retries_setup::run_dsub () {
  local -r job_name="${1}"
  local -r retries="${2}"
  local -r command="${3}"

  if run_dsub \
      --name "${job_name}" \
      --label test-token="${TEST_TOKEN}" \
      --command "${command}" \
      --retries "${retries}" \
      --wait; then
    if [[ "${command}" == "false" ]]; then
      echo "dsub did not have non-zero exit code as expected."
      exit 1
    fi
  else
    if [[ "${command}" == "true" ]]; then
      echo "dsub had non-zero exit code and should not have."
      exit 1
    fi
  fi
}
readonly -f retries_setup::run_dsub

function retries_setup::run_dsub_preemptible () {
  local -r job_name="${1}"
  local -r retries="${2}"
  local -r preemptible_retries="${3}"
  local -r command="${4}"

  if run_dsub \
      --name "${job_name}" \
      --label test-token="${TEST_TOKEN}" \
      --command "${command}" \
      --retries "${retries}" \
      --preemptible "${preemptible_retries}" \
      --wait; then
    if [[ "${command}" == "false" ]]; then
      echo "dsub did not have non-zero exit code as expected."
      exit 1
    fi
  else
    if [[ "${command}" == "true" ]]; then
      echo "dsub had non-zero exit code and should not have."
      exit 1
    fi
  fi
}
readonly -f retries_setup::run_dsub_preemptible

function retries_setup::check_job_attr() {
  local -r job_name="${1}"
  local -r attr="${2}"
  local -r expected_values="${3}"

  local -r dstat_out="$(
    run_dstat \
      --names "${job_name}" \
      --label test-token="${TEST_TOKEN}" \
      --status '*' \
      --full \
      --format yaml)"

  # Check the expected values
  echo "Checking expected values."
  local num=0
  local expected
  for expected in ${expected_values}; do
    local result="$(
      python "${SCRIPT_DIR}"/get_data_value.py \
        yaml "${dstat_out}" "[${num}].${attr}")"

    if [[ "${result}" != "${expected}" ]]; then
      echo "Unexpected ${attr} for job ${job_name}"
      echo "Result: ${result}"
      echo "Expected: ${expected}"
      echo "${dstat_out}"
      exit 1
    fi

    : $(( num = num + 1 ))
  done

  # Check that there were no extra attempts
  echo "Checking that there are no unexpected attempts"
  local -r beyond="$(
      python "${SCRIPT_DIR}"/get_data_value.py \
        yaml "${dstat_out}" "[${num}].${attr}")"
  if [[ -n "${beyond}" ]]; then
    echo "Unexpected attempt for job ${job_name}"
    echo "${dstat_out}"
    exit 1
  fi
}
readonly -f retries_setup::check_job_attr
