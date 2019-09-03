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

# Verify the ability to delete a batch of tasks.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Utility routine for getting a list of task statuses
function get_tasks_status() {
  local job_id="${1}"

  run_dstat --jobs "${job_id}" --status '*' --full --format yaml | \
    sed --quiet 's#^ *status: *\(.*\)#\1#p'
}
readonly -f get_tasks_status

echo "Launching pipelines..."

JOB_ID="$(run_dsub \
  --command 'sleep 5m' \
  --tasks "${TASKS_FILE}")"

# Get a count of the number of lines in the tasks file.
# It should be one more than the total number of tasks, since it includes
# the header line.
readonly TASKS_FILE_LINE_COUNT="$(cat "${TASKS_FILE}" | wc -l)"
readonly EXPECTED_TASKS_COUNT="$((TASKS_FILE_LINE_COUNT - 1))"

# Get a count of the number of tasks launched
declare TASKS_STATUS="$(get_tasks_status "${JOB_ID}")"
readonly TASKS_LAUNCH_COUNT="$(echo "${TASKS_STATUS}" | wc -l)"

echo "Tasks launched: ${TASKS_LAUNCH_COUNT}"

if [[ "${TASKS_LAUNCH_COUNT}" -ne "${EXPECTED_TASKS_COUNT}" ]]; then
  1>&2 echo "Unexpected count of launched tasks: ${TASKS_LAUNCH_COUNT}"
  1>&2 echo "Expected count of launched tasks: ${EXPECTED_TASKS_COUNT}"
  exit 1
fi

echo
echo "Current task statuses:"
echo "${TASKS_STATUS}" | sort | uniq --count

# The local runner can take a short while to get the docker containers
# running. When we request killing the task, if it is not there, it will
# generate a "No such container" error. The provider's behavior is not
# *wrong*, but it makes this test noisy. Sleep a short moment to let the
# containers get up and running.
sleep 5s

echo
echo "Canceling pipelines"

run_ddel --jobs "${JOB_ID}"

echo "Querying pipelines"

# It can take a while for a job to reach fully canceled status.
readonly TOTAL_WAIT_SECONDS="$((60 * 3))"
readonly WAIT_INTERVAL=5

for ((WAITED = 0; WAITED <= TOTAL_WAIT_SECONDS; WAITED += WAIT_INTERVAL)); do
  TASKS_STATUS="$(get_tasks_status "${JOB_ID}")"

  echo
  echo "Current task statuses:"
  echo "${TASKS_STATUS}" | sort | uniq --count

  # Check that all tasks have stopped running.
  RUNNING_TASK_COUNT="$(echo "${TASKS_STATUS}" | grep 'RUNNING' | wc -l)"
  if [[ "${RUNNING_TASK_COUNT}" -eq 0 ]]; then
    break
  fi

  echo
  echo "Sleep ${WAIT_INTERVAL} seconds to wait for all tasks to be canceled."
  sleep "${WAIT_INTERVAL}"
done

if [[ "${WAITED}" -ge "${TOTAL_WAIT_SECONDS}" ]]; then
  1>&2 echo "Still waiting for tasks to reach CANCELED state after ${TOTAL_WAIT_SECONDS}"
  1>&2 echo "FAILED"
  exit 1
fi

echo "SUCCESS"
