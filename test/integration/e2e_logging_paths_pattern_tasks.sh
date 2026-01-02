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

# Test that the provider sets up logging paths correctly for a formatted string

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do logging paths setup
source "${SCRIPT_DIR}/logging_paths_tasks_setup.sh"

readonly LOGGING_BASE="$(dirname "${LOGGING}")"
declare LOGGING_OVERRIDE

readonly JOB_NAME=$(logging_paths_tasks_setup::get_job_name)
readonly JOB_USER="${USER:-$(whoami)}"

# Set up the tasks file
logging_paths_tasks_setup::write_tasks_file

# Launch the job
LOGGING_OVERRIDE="${LOGGING_BASE}/{user-id}/{job-name}.{task-id}.test.log"
JOB_ID=$(logging_paths_tasks_setup::run_dsub \
           --user "${JOB_USER}")

# Verify output
LOGGING_PATH=$(logging_paths_tasks_setup::dstat_get_logging "${JOB_ID}" "1")

if [[ ! "${LOGGING_PATH}" == "${LOGGING_BASE}/${JOB_USER}/${JOB_NAME}.1.test.log" ]]; then
  echo "ERROR: Unexpected logging path."
  echo "Received: ${LOGGING_PATH}"
  echo "Expected: ${LOGGING_BASE}/${JOB_USER}/${JOB_NAME}.1.test.log"
  exit 1
fi

# Cancel the task
logging_paths_tasks_setup::ddel_task "${JOB_ID}"

echo
echo "SUCCESS: Logging path: ${LOGGING_PATH}"
echo
