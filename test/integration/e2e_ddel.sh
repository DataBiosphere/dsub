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

function get_status() {
  local JOBID="$1"
  local STATUS=$(run_dstat --jobs "${JOBID}" --status "*")
  # if the job's pending, then wait.
  while echo "${STATUS}" | grep -qi 'pending' - ; do
    sleep 5s
    STATUS=$(run_dstat --jobs "${JOBID}" --status "*")
  done
  echo "${STATUS}"
}

function provider_verify_stopped_job() {
  # For the local provider we can do white-box testing
  # to make sure Docker behavior matches what dstat is saying.
  if [[ "${DSUB_PROVIDER}" == "local" ]]; then
    local FOLDER="${TMPDIR:-/tmp}/dsub-local/${JOBID}/task"
    echo "Making sure no more output is being generated."
    local COUNT1="$(wc -l "${FOLDER}/stdout.txt")"
    sleep 4
    local COUNT2="$(wc -l "${FOLDER}/stdout.txt")"
    if [[ "${COUNT1}" != "${COUNT2}" ]]; then
      echo "new output has been generated *after* the pipeline was killed!"
      echo "Before: $COUNT1"
      echo "After:  $COUNT2"
      exit 1
    fi
  fi
}

# Test ddel.
#
# Makes sure that dstat reports the job as canceled after we cancel it,
# and (for the local provider) checks that no more output's generated.

readonly SCRIPT_DIR="$(dirname "${0}")"

source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOBID=$(run_dsub \
    --command "for ((i=0; i < 600; i++)); do echo beep ; sleep 1s ; done")

  echo "Pipeline started."
  echo "job id: ${JOBID}"

  # Make sure dstat shows the job as not canceled
  # (as a side effect this waits until it's not pending anymore)
  STATUS="$(get_status "${JOBID}")"
  if echo "${STATUS}" | grep -i 'canceled' - ; then
    echo "Operation canceled itself!"
    echo "${STATUS}"
    exit 1
  fi

  echo "Killing the pipeline"
  run_ddel --jobs "${JOBID}"

  # Make sure dstat shows the job as canceled.
  # In the case of the google provider there may be a delay.
  STATUS="$(get_status "${JOBID}")"
  if ! echo "${STATUS}" | grep -qi 'canceled' - ; then
    echo "dstat does not show the operation as canceled!"
    echo "${STATUS}"
    echo "5 minutes grace period..."
    # job runs for 10min, giving it 5min to cancel.
    for ((i=1; i <= 30; i++)); do
      sleep 10s
      STATUS="$(get_status "${JOBID}")"
      if echo "${STATUS}" | grep -qi 'canceled' - ; then
        echo "OK after $((i * 10)) seconds."
        break
      fi
    done
    if ! echo "${STATUS}" | grep -qi 'canceled' - ; then
      echo "dstat does not show the operation as canceled!"
      echo "${STATUS}"
      exit 1
    fi
  fi

  provider_verify_stopped_job

  echo "SUCCESS"

fi


