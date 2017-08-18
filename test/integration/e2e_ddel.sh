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

# get_status
#
# Run dstat and return the "status" field for the specified job.
function get_status() {
  local job_id="$1"

  local dstat_out=$(\
    run_dstat \
      --jobs "${job_id}" \
      --status "*" \
      --age 30m \
      --full \
      --format json)

  python "${SCRIPT_DIR}"/get_json_value.py \
    "${dstat_out}" "[0].status"
}
readonly -f get_status

# wait_for_canceled_status
#
# Wait a maximum number of seconds for a job to reach canceled status.
# If it is canceled status, then return 0, otherwise if it the maximum wait
# is reached, return 1.
function wait_for_canceled_status() {
  local job_id="${1}"

  # After calling ddel, we wait a short bit for canceled status.
  # For most providers, this should be very fast, but the Google Pipelines API
  # "operations.cancel" will return success when the operation is internally
  # marked for deletion and there can be a short delay before it is externally
  # marked as CANCELED.
  local max_wait_sec=10
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    max_wait_sec=90
  fi

  echo "Waiting up to ${max_wait_sec} sec for CANCELED status of ${job_id}"
  for ((sec = 0; sec < max_wait_sec; sec += 5)); do
    local status="$(get_status "${job_id}")"
    if [[ "${status}" == "CANCELED" ]]; then
      return 0
    fi
    echo "Status: ${status}. Sleep 5s"
    sleep 5s
  done

  return 1
}
readonly -f wait_for_canceled_status

function provider_verify_stopped_job() {
  local job_id="${1}"

  # For the local provider we can do white-box testing
  # to make sure Docker behavior matches what dstat is saying.
  if [[ "${DSUB_PROVIDER}" == "local" ]]; then
    local folder="${TMPDIR:-/tmp}/dsub-local/${job_id}/task"

    echo "Making sure no more output is being generated."
    local count1="$(wc -l "${folder}/stdout.txt")"
    sleep 4
    local count2="$(wc -l "${folder}/stdout.txt")"

    if [[ "${count1}" != "${count2}" ]]; then
      echo "new output has been generated *after* the pipeline was killed!"
      echo "Before: ${count1}"
      echo "After:  ${count2}"
      exit 1
    fi
  fi
}
readonly -f provider_verify_stopped_job

# Test ddel.
#
# Makes sure that dstat reports the job as canceled after we cancel it,
# and (for the local provider) checks that no more output's generated.

readonly SCRIPT_DIR="$(dirname "${0}")"

source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOB_ID=$(run_dsub \
    --name "ddel-test" \
    --command "for ((i=0; i < 600; i++)); do echo 'test' ; sleep 1s ; done")

  echo
  echo "Pipeline started."
  echo "job id: ${JOB_ID}"

  # Test ddel with an age that is too soon
  echo
  echo "Sleeping 10 seconds before exercising 'ddel --age 5s'"
  sleep 10s
  run_ddel --jobs "${JOB_ID}" --age 5s

  # Make sure dstat still shows the job as not canceled.
  echo
  echo "Check that the job is not canceled."
  if wait_for_canceled_status "${JOB_ID}"; then
    echo "ERROR: Operation is canceled, but ddel should not have canceled it."
    get_status "${JOB_ID}"
    exit 1
  fi

  echo
  echo "Killing the pipeline"
  run_ddel --jobs "${JOB_ID}"

  # Make sure dstat shows the job as canceled.
  if ! wait_for_canceled_status "${JOB_ID}"; then
    echo "dstat does not show the operation as canceled after wait."
    get_status "${JOB_ID}"
    exit 1
  fi

  echo
  echo "dstat indicates that the job is canceled."

  provider_verify_stopped_job "${JOB_ID}"

  echo "SUCCESS"

fi
