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

  if [[ "${DSUB_PROVIDER}" != "google" ]]; then
    # v1alpha2 of the Google Pipelines API can take a long time to fully cancel
    # a pipeline if it has already started running.
    # So skip the "fake cancel and wait" for the google provider for now.

    # Test ddel with an age that is too soon
    echo
    echo "Sleeping 10 seconds before exercising 'ddel --age 5s'"
    sleep 10s
    run_ddel_age "5s" --jobs "${JOB_ID}"

    # Make sure dstat still shows the job as not canceled.
    echo
    echo "Check that the job is not canceled."
    if util::wait_for_canceled_status "${JOB_ID}"; then
      echo "ERROR: Operation is canceled, but ddel should not have canceled it."
      util::get_job_status "${JOB_ID}"
      exit 1
    fi

    # For the google v2 providers, wait a sufficiently long time so that all the
    # startup events occur prior to canceling so that the output event list is
    # consistent..
    if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]] || \
       [[ "${DSUB_PROVIDER}" == "google-v2" ]]; then
      echo "Sleeping for 60s"
      sleep 60
    fi
  fi

  echo
  echo "Killing the pipeline"
  run_ddel --jobs "${JOB_ID}"

  # Make sure dstat shows the job as canceled.
  if ! util::wait_for_canceled_status "${JOB_ID}"; then
    echo "dstat does not show the operation as canceled after wait."
    util::get_job_status "${JOB_ID}"
    exit 1
  fi

  echo
  echo "dstat indicates that the job is canceled."

  # Check that there is a valid end time
  DSTAT_OUTPUT=$(run_dstat --status '*' --jobs "${JOB_ID}" --full)
  if ! util::dstat_yaml_job_has_valid_end_time "${DSTAT_OUTPUT}"; then
    echo "dstat output for ${JOB_ID} does not include a valid end time."
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  # The 'google' provider job is canceled immediately after start so no events
  # are registered.
  if [[ "${DSUB_PROVIDER}" != "google" ]]; then
    util::dstat_yaml_assert_field_equal "${DSTAT_OUTPUT}" "[0].events.[-1].name" "canceled"
  fi

  provider_verify_stopped_job "${JOB_ID}"

  echo "SUCCESS"

fi
