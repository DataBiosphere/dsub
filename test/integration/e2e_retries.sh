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

# Test the --retries dsub flag.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly JOB_NAME_PASS=job-pass
readonly JOB_NAME_FAIL1=job-fail1
readonly JOB_NAME_FAIL2=job-fail2

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then
  function run_dsub_retry () {
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

  echo "Launch a job that should succeed with --retries 2"
  run_dsub_retry "${JOB_NAME_PASS}" 2 true
  echo "Launch a job that should fail with --retries 1"
  run_dsub_retry "${JOB_NAME_FAIL1}" 1 false
  echo "Launch a job that should fail with --retries 2"
  run_dsub_retry "${JOB_NAME_FAIL2}" 2 false
fi

function check_job_attr() {
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
  local -r beyond="$(
      python "${SCRIPT_DIR}"/get_data_value.py \
        yaml "${dstat_out}" "[${num}].${attr}")"
  if [[ -n "${beyond}" ]]; then
    echo "Unexpected attempt for job ${job_name}"
    echo "${dstat_out}"
    exit 1
  fi
}

echo

# Note: dstat returns in reverse order.
echo "Checking task metadata for job that should succeed with --retries 2..."
check_job_attr "${JOB_NAME_PASS}" status "SUCCESS"
check_job_attr "${JOB_NAME_PASS}" task-attempt "1"
echo "Checking task metadata for job that should fail with --retries 1..."
check_job_attr "${JOB_NAME_FAIL1}" status "FAILURE FAILURE"
check_job_attr "${JOB_NAME_FAIL1}" task-attempt "2 1"
echo "Checking task metadata for job that should fail with --retries 2..."
check_job_attr "${JOB_NAME_FAIL2}" status "FAILURE FAILURE FAILURE"
check_job_attr "${JOB_NAME_FAIL2}" task-attempt "3 2 1"

echo "SUCCESS"

