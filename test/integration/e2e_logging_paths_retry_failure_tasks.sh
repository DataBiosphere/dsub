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

# Test that the provider sets up logging paths correctly for a failing retry job

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do logging paths setup
source "${SCRIPT_DIR}/logging_paths_tasks_setup.sh"

readonly LOGGING_BASE="$(dirname "${LOGGING}")"
declare LOGGING_OVERRIDE

readonly JOB_NAME=$(logging_paths_tasks_setup::get_job_name)

# Set up the tasks file
logging_paths_tasks_setup::write_tasks_file

# Launch the job
LOGGING_OVERRIDE="${LOGGING_BASE}"
JOB_ID=$(logging_paths_tasks_setup::run_dsub \
           --retries 1 \
           --wait \
           --command 'false') || true

# Verify output
for task_attempt in 1 2; do
  LOGGING_PATH=$(logging_paths_tasks_setup::dstat_get_logging "${JOB_ID}" "${task_attempt}")

  if [[ ! "${LOGGING_PATH}" == "${LOGGING_OVERRIDE}/${JOB_NAME}"*.1."${task_attempt}".log ]]; then
    echo "ERROR: Unexpected logging path."
    echo "Received: ${LOGGING_PATH}"
    echo "Expected: ${LOGGING_OVERRIDE}/${JOB_NAME}*.1.${task_attempt}.log"
    exit 1
  fi

  echo
  echo "SUCCESS: Logging path: ${LOGGING_PATH}"
  echo
done
