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
  local STATUS="$(run_dstat --jobs "${JOBID}" --status "*")"
  # if the job's pending, then wait.
  while echo "${STATUS}" | grep -qi 'pending' - ; do
    sleep 5s
    STATUS="$(run_dstat --jobs "${JOBID}" --status "*")"
  done
  echo "${STATUS}"
}

# Test dstat.
#
# This test launches a single job and then simply verifies that dstat,
# in both the default and "--full" modes, returns without error and
# the output looks minimally sane.

readonly SCRIPT_DIR="$(dirname "${0}")"
readonly JOB_NAME="TEST-JOB"

# This test is not sensitive to the output of the dsub job.
# Set the ALLOW_DIRTY_TESTS environment variable to 1 in your shell to
# run this test without first emptying the output and logging directories.
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOBID="$(run_dsub \
    --name "${JOB_NAME}" \
    --command "echo 'hello world'")"

  echo "Checking dstat (default)..."

  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --jobs "${JOBID}" 2>&1)"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${JOB_NAME}"; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Checking dstat (full)..."

  if ! DSTAT_OUTPUT=$(run_dstat --status '*' --full --jobs "${JOBID}" 2>&1); then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if ! echo "${DSTAT_OUTPUT}" | grep -qi "${JOB_NAME}"; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "SUCCESS"

fi


