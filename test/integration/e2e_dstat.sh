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
# This test launches a single job and then verifies that dstat
# can lookup jobs by job-id, status, age, and job-name, with
# both default and --full output. It ensures that no error is
# returned and the output looks minimally sane.

readonly SCRIPT_DIR="$(dirname "${0}")"
readonly JOB_NAME="test-job"

function dstat_output_job_name() {
  local dstat_out="${1}"

  python "${SCRIPT_DIR}"/get_data_value.py \
    "yaml" "${dstat_out}" "[0].job-name"
}
readonly -f dstat_output_job_name

# This test is not sensitive to the output of the dsub job.
# Set the ALLOW_DIRTY_TESTS environment variable to 1 in your shell to
# run this test without first emptying the output and logging directories.
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOBID="$(run_dsub \
    --name "${JOB_NAME}" \
    --command 'sleep 1m')"

  echo "Checking dstat (by status)..."

  if ! DSTAT_OUTPUT="$(test_dstat --status 'RUNNING' --jobs "${JOBID}" --full)"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if [[ "$(dstat_output_job_name "${DSTAT_OUTPUT}")" != "${JOB_NAME}" ]]; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Checking dstat (by job-name)..."

  if ! DSTAT_OUTPUT="$(test_dstat --status '*' --full --names "${JOB_NAME}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if [[ "$(dstat_output_job_name "${DSTAT_OUTPUT}")" != "${JOB_NAME}" ]]; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Checking dstat (by job-id: default)..."

  if ! DSTAT_OUTPUT="$(test_dstat --status '*' --jobs "${JOBID}")"; then
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

  echo "Checking dstat (by job-id: full)..."

  if ! DSTAT_OUTPUT=$(test_dstat --status '*' --full --jobs "${JOBID}"); then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  if [[ "$(dstat_output_job_name "${DSTAT_OUTPUT}")" != "${JOB_NAME}" ]]; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Waiting 5 seconds and checking 'dstat --age 5s'..."
  sleep 5s

  DSTAT_OUTPUT="$(run_dstat --status '*' --jobs "${JOBID}" --age 5s --full)"
  if [[ "${DSTAT_OUTPUT}" != "[]" ]]; then
    echo "dstat output not empty as expected:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "Verifying that the job didn't disappear completely."

  DSTAT_OUTPUT="$(test_dstat --status '*' --jobs "${JOBID}" --full)"
  if [[ "$(dstat_output_job_name "${DSTAT_OUTPUT}")" != "${JOB_NAME}" ]]; then
    echo "Job ${JOB_NAME} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  echo "SUCCESS"

fi


