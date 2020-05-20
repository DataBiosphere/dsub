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
  local ensure_complete="${2:-}"

  # Verify that that the jobs are found and are in the expected order.
  # dstat sort ordering is by create-time (descending), so job 0 here should be the last started.
  local first_job_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[0].job-name")"
  local second_job_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[1].job-name")"
  local third_job_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[2].job-name")"

  if [[ "${first_job_name}" != "${RUNNING_JOB_NAME_2}" ]]; then
    1>&2 echo "Job ${RUNNING_JOB_NAME_2} not found in the correct location in the dstat output! "
    1>&2 echo "${dstat_out}"
    exit 1
  fi

  if [[ "${second_job_name}" != "${RUNNING_JOB_NAME}" ]]; then
    1>&2 echo "Job ${RUNNING_JOB_NAME} not found in the correct location in the dstat output!"
    1>&2 echo "${dstat_out}"
    exit 1
  fi

  if [[ "${third_job_name}" != "${COMPLETED_JOB_NAME}" ]]; then
    1>&2 echo "Job ${COMPLETED_JOB_NAME} not found in the correct location in the dstat output!"
    1>&2 echo "${dstat_out}"
    exit 1
  fi

  # Check that all "events" are present.
  # By the time this runs, the first job launched has completed (we --wait).
  # By the time we run this for the final time, all jobs have completed.
  local check_completed_tasks="2"
  if [[ "${ensure_complete}" == "true" ]]; then
    check_completed_tasks="0 1 2"
  fi

  local expected_events=(start pulling-image localizing-files running-docker delocalizing-files ok)
  for task in ${check_completed_tasks}; do
    util::dstat_out_assert_equal_events "${dstat_out}" "[${task}].events" "${expected_events[@]}"
  done

  # Check provider-specific fields
  if [[ "${DSUB_PROVIDER}" == "google" ]] || \
     [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]] || \
     [[ "${DSUB_PROVIDER}" == "google-v2" ]]; then
    echo "Checking dstat ${DSUB_PROVIDER} provider fields"
    verify_dstat_google_provider_fields "${dstat_out}" "${ensure_complete}"
  fi
}
readonly -f verify_dstat_output


function verify_dstat_google_provider_fields() {
  local dstat_out="${1}"
  local ensure_complete="${2:-}"

  for (( task=0; task < 3; task++ )); do
    # Run the provider test.
    local job_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${task}].job-name")"
    local job_provider="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${task}].provider")"

    # Validate provider.
    if [[ "${job_provider}" != "${DSUB_PROVIDER}" ]]; then
      1>&2 echo "  - FAILURE: provider ${job_provider} does not match '${DSUB_PROVIDER}'"
      1>&2 echo "${dstat_out}"
      exit 1
    fi

    # For google-cls-v2, validate that the correct "location" was used for the request.
    if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]]; then
      local op_name="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${DSTAT_OUTPUT}" "[0].internal-id")"

      # The operation name format is projects/<project-number>/locations/<location>/operations/<operation-id>
      local op_location="$(echo -n "${op_name}" | awk -F '/' '{ print $4 }')"
      if [[ "${op_location}" != "${LOCATION}" ]]; then
        1>&2 echo "Location incorrect: ${op_location} instead of ${LOCATION}"
        exit 1
      fi
    fi

    # Provider fields are both metadata set on task submission and machine
    # information set when the Pipelines API starts processing the task.

    # Some information should be available immediately, while other information
    # is only available once the task has been started.

    # For simplicity, let's just check when the tasks are complete.

    # Check boot disk: expect default of 10
    util::dstat_yaml_assert_field_equal "${dstat_out}" "[${task}].provider-attributes.boot-disk-size" 10

    # Check data disk: expect default of 200, pd-standard
    util::dstat_yaml_assert_field_equal "${dstat_out}" "[${task}].provider-attributes.disk-size" 200
    util::dstat_yaml_assert_field_equal "${dstat_out}" "[${task}].provider-attributes.disk-type" "pd-standard"

    # Check machine-type: expect default of n1-standard-1
    util::dstat_yaml_assert_field_equal "${dstat_out}" "[${task}].provider-attributes.machine-type" "n1-standard-1"

    # Check preemptible: expect default of "false"
    util::dstat_yaml_assert_boolean_field_equal "${dstat_out}" "[${task}].provider-attributes.preemptible" "false"

    # Check that instance name is not empty
    local instance_name=$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${task}].provider-attributes.instance-name")
    if [[ -z "${instance_name}" ]]; then
      1>&2 echo "  - FAILURE: Instance ${instance_name} for job ${job_name}, task $((task+1)) is empty."
      1>&2 echo "${dstat_out}"
      exit 1
    fi

    # Check zone exists and is expected format
    local job_zone=$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${dstat_out}" "[${task}].provider-attributes.zone")
    if ! [[ "${job_zone}" =~ ^[a-z]{1,4}-[a-z]{2,15}[0-9]-[a-z]$ ]]; then
      1>&2 echo "  - FAILURE: Zone ${job_zone} for job ${job_name}, task $((task+1)) not valid."
      1>&2 echo "${dstat_out}"
      exit 1
    fi
  done

  echo "  - ${DSUB_PROVIDER} provider fields verified"
}
readonly -f verify_dstat_google_provider_fields


# This test is not sensitive to the output of the dsub job.
# Set the ALLOW_DIRTY_TESTS environment variable to 1 in your shell to
# run this test without first emptying the output and logging directories.
source "${SCRIPT_DIR}/test_setup_e2e.sh"


if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  # For google-cls-v2, we will test that the "--location" parameter works by
  # specifying something other than the default (us-central1) and then verify
  # that the operation name (which is region-specific) includes the test region.
  if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]]; then
    export LOCATION=us-west2
  fi

  echo "Launching pipeline..."

  COMPLETED_JOB_ID="$(run_dsub \
    --name "${COMPLETED_JOB_NAME}" \
    --label test-token="${TEST_TOKEN}" \
    --command 'echo TEST')"

  RUNNING_JOB_ID="$(run_dsub \
    --name "${RUNNING_JOB_NAME}" \
    --label test-token="${TEST_TOKEN}" \
    --command 'sleep 10s')"

  RUNNING_JOB_ID_2="$(run_dsub \
    --name "${RUNNING_JOB_NAME_2}" \
    --label test-token="${TEST_TOKEN}" \
    --command 'sleep 20s')"

  echo ""
  echo "Waiting for ${COMPLETED_JOB_ID} to complete."
  echo ""
  run_dstat --jobs "${COMPLETED_JOB_ID}" --wait

  echo ""
  echo "Job completed: ${COMPLETED_JOB_ID}. Begin verifications."

  echo "Checking dstat (by status)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status 'RUNNING' 'SUCCESS' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    1>&2 echo "dstat exited with a non-zero exit code!"
    1>&2 echo "Output:"
    1>&2 echo "${DSTAT_OUTPUT}"
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
    1>&2 echo "dstat exited with a non-zero exit code!"
    1>&2 echo "Output:"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "Checking dstat (by job-id: default)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    1>&2 echo "dstat exited with a non-zero exit code!"
    1>&2 echo "Output:"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${RUNNING_JOB_NAME}"; then
    1>&2 echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${RUNNING_JOB_NAME_2}"; then
    1>&2 echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${COMPLETED_JOB_NAME}"; then
    1>&2 echo "Job ${RUNNING_JOB_NAME} not found in the dstat output!"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Checking dstat (by job-id: full)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    1>&2 echo "dstat exited with a non-zero exit code!"
    1>&2 echo "Output:"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "Checking dstat (by repeated job-ids: full)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"; then
    1>&2 echo "dstat exited with a non-zero exit code!"
    1>&2 echo "Output:"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  verify_dstat_output "${DSTAT_OUTPUT}"

  echo "Waiting 5 seconds and checking 'dstat --age 5s'..."
  sleep 5s

  DSTAT_OUTPUT="$(run_dstat_age "5s" --status '*' --full --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"
  if [[ "${DSTAT_OUTPUT}" != "[]" ]]; then
    1>&2 echo "dstat output not empty as expected:"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Waiting for all jobs to complete."

  DSTAT_OUTPUT="$(run_dstat --status '*' --full --wait --jobs "${RUNNING_JOB_ID_2}" "${RUNNING_JOB_ID}" "${COMPLETED_JOB_ID}")"
  verify_dstat_output "${DSTAT_OUTPUT}" "true"

  echo "SUCCESS"

fi


