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

# Test that the provider sets up logging paths correctly

# Do standard test setup
readonly SCRIPT_DIR="$(dirname "${0}")"
source "${SCRIPT_DIR}/test_setup_e2e.sh"

function dstat_get_logging() {
  local job_id="${1}"

  local dstat_out=$(\
    run_dstat \
      --jobs "${job_id}" \
      --status "*" \
      --age 30m \
      --full \
      --format json)

  python "${SCRIPT_DIR}"/get_json_value.py \
    "${dstat_out}" "[0].logging"
}
readonly -f dstat_get_logging

readonly LOGGING_BASE="$(dirname "${LOGGING}")"
declare LOGGING_OVERRIDE

readonly JOB_NAME="log-test"
readonly JOB_USER="${USER}"

# Test a basic job with base logging path
echo "Subtest #1: Basic logging path"

LOGGING_OVERRIDE="${LOGGING_BASE}"
JOB_ID=$(run_dsub \
           --name "${JOB_NAME}" \
           --command 'echo "Test"')

LOGGING_PATH=$(dstat_get_logging "${JOB_ID}")

if [[ ! "${LOGGING_PATH}" == "${LOGGING_OVERRIDE}/log-test"*.log ]]; then
  echo "ERROR: Unexpected logging path."
  echo "Received: ${LOGGING_PATH}"
  echo "Expected: ${LOGGING_OVERRIDE}/log-test*.log"
  exit 1
fi

echo
echo "SUCCESS: Logging path: ${LOGGING_PATH}"
echo

# Test a basic job with base logging path
echo "Subtest #2: Basic logging path ending in .log"

LOGGING_OVERRIDE="${LOGGING_BASE}/task.log"
JOB_ID=$(run_dsub \
           --name "${JOB_NAME}" \
           --command 'echo "Test"')

LOGGING_PATH=$(dstat_get_logging "${JOB_ID}")

if [[ ! "${LOGGING_PATH}" == "${LOGGING_OVERRIDE}" ]]; then
  echo "ERROR: Unexpected logging path."
  echo "Received: ${LOGGING_PATH}"
  echo "Expected: ${LOGGING_OVERRIDE}"
  exit 1
fi

echo
echo "SUCCESS: Logging path: ${LOGGING_PATH}"
echo

# Test a basic job with a full format string
echo "Subtest #3: Logging path with a pattern"

LOGGING_OVERRIDE="${LOGGING_BASE}/{user-id}/{job-name}.test.log"
JOB_ID=$(run_dsub \
           --name "${JOB_NAME}" \
           --user "${JOB_USER}" \
           --command 'echo "Test"')

LOGGING_PATH=$(dstat_get_logging "${JOB_ID}")

if [[ ! "${LOGGING_PATH}" == "${LOGGING_BASE}/${JOB_USER}/${JOB_NAME}.test.log" ]]; then
  echo "ERROR: Unexpected logging path."
  echo "Received: ${LOGGING_PATH}"
  echo "Expected: ${LOGGING_BASE}/${JOB_USER}/${JOB_NAME}.test.log"
  exit 1
fi

echo
echo "SUCCESS: Logging path: ${LOGGING_PATH}"
echo
