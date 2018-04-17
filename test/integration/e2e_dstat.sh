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

# Test dstat.
#
# This test launches three jobs and then verifies that dstat
# can lookup jobs by job-id, status, age, and job-name, with
# both default and --full output. It ensures that no error is
# returned and the output looks minimally sane.

readonly SCRIPT_DIR="$(dirname "${0}")"
readonly COMPLETED_JOB_NAME="completed-job"
readonly RUNNING_JOB_NAME="running-job"
readonly RUNNING_JOB_NAME_2="running-job-2"

function verify_dstat_output() {
  local dstat_out="${1}"

  # Verify that that the jobs are found and are in the expected order.
  # dstat sort ordering is by create-time (descending), so job 0 here should be the last started.
  FIRST_JOB_NAME="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[0].job-name")"
  SECOND_JOB_NAME="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[1].job-name")"
  THIRD_JOB_NAME="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[2].job-name")"

  if [[ "${FIRST_JOB_NAME}" != "${RUNNING_JOB_NAME_2}" ]]; then
    echo "Job ${RUNNING_JOB_NAME_2} not found in the correct location in the dstat output! "
    echo "${dstat_out}"
    exit 1
  fi

  if [[ "${SECOND_JOB_NAME}" != "${RUNNING_JOB_NAME}" ]]; then
    echo "Job ${RUNNING_JOB_NAME} not found in the correct location in the dstat output!"
    echo "${dstat_out}"
    exit 1
  fi

  if [[ "${THIRD_JOB_NAME}" != "${COMPLETED_JOB_NAME}" ]]; then
    echo "Job ${COMPLETED_JOB_NAME} not found in the correct location in the dstat output!"
    echo "${dstat_out}"
    exit 1
  fi
}
readonly -f verify_dstat_output


function verify_dstat_google_provider_fields() {
   local dstat_out="${1}"
   for (( job=0; job < 3; job++ )); do
     # Skip the test if the job's state is not yet running or completed.
     local status="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${job}].status")"
     if ! [[ "${status}" =~ ^(RUNNING|SUCCESS)$ ]]; then
       echo "  - NOTICE: status '${status}' may not have compute metadata,"
       echo "            skipping provider fields tests."
       exit 0
     fi

     # Run the provider test.
     local job_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${job}].job-name")"
     local job_provider="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${job}].provider")"

     # Validate provider.
     if [[ "${job_provider}" != "${DSUB_PROVIDER}" ]]; then
       echo "  - FAILURE: provider ${job_provider} does not match '${DSUB_PROVIDER}'"
       echo "${dstat_out}"
       exit 1
     fi
     local job_zone=$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${job}].provider-attributes.zone")
     if ! [[ "${job_zone}" =~ ^[a-z]{1,4}-[a-z]{2,15}[0-9]-[a-z]$ ]]; then
       echo "  - FAILURE: Job zone ${job_zone} for job ${job_name} not valid."
       echo "${dstat_out}"
       exit 1
     fi
   done
   echo "  - google provider fields verified"
}
readonly -f verify_dstat_google_provider_fields


# This test is not sensitive to the output of the dsub job.
# Set the ALLOW_DIRTY_TESTS environment variable to 1 in your shell to
# run this test without first emptying the output and logging directories.
source "${SCRIPT_DIR}/test_setup_e2e.sh"


if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  COMPLETED_JOB_ID="$(run_dsub \
    --name "${COMPLETED_JOB_NAME}" \
    --command 'echo TEST' \
    --label test-token="${TEST_TOKEN}" \
    --wait)"

  RUNNING_JOB_ID="$(run_dsub \
    --name "${RUNNING_JOB_NAME}" \
    --label test-token="${TEST_TOKEN}" \
    --command 'sleep 1m')"

  RUNNING_JOB_ID_2="$(run_dsub \
    --name "${RUNNING_JOB_NAME_2}" \
    --label test-token="${TEST_TOKEN}" \
    --command 'sleep 1m')"

  echo "Checking dstat (by status)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status 'RUNNING' 'SUCCESS' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "Checking dstat (by job-name)..."

  # For the google provider, sleep briefly to allow the Pipelines v1
  # to set the compute properties, which occurs shortly after pipeline submit.
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    sleep 2
  fi

  if ! DSTAT_OUTPUT="$(run_dstat --status 'RUNNING' 'SUCCESS' --full --names "${RUNNING_JOB_NAME_2}" "${RUNNING_JOB_NAME}" "${COMPLETED_JOB_NAME}" --label "test-token=${TEST_TOKEN}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    echo "Checking dstat google provider fields"
    verify_dstat_google_provider_fields "${DSTAT_OUTPUT}"
  fi

  echo "Checking dstat (by job-id: default)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${RUNNING_JOB_NAME}"; then
    echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${RUNNING_JOB_NAME_2}"; then
    echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${COMPLETED_JOB_NAME}"; then
    echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Checking dstat (by job-id: full)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    echo "Checking dstat google provider fields"
    verify_dstat_google_provider_fields "${DSTAT_OUTPUT}"
  fi

  echo "Checking dstat (by repeated job-ids: full)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "Waiting 5 seconds and checking 'dstat --age 5s'..."
  sleep 5s

  DSTAT_OUTPUT="$(run_dstat_age "5s" --status '*' --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}" --full)"
  if [[ "${DSTAT_OUTPUT}" != "[]" ]]; then
    echo "dstat output not empty as expected:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Verifying that the job didn't disappear completely."

  DSTAT_OUTPUT="$(run_dstat --status '*' --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}" --full)"
  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "SUCCESS"

fi


