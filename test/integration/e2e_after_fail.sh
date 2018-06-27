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

# Test the --after and --wait flags when dsub jobs fail

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  # (1) Launch a job to test command execution failure
  echo "Launch a job that should fail (--wait for it)..."
  if JOBID="$(run_dsub \
      --command 'exit 1' \
      --wait)"; then
    echo "Expected dsub to exit with error."
    exit 1
  elif [[ -z "${JOBID}" ]]; then
    echo "dsub did not report a jobid but it should have."
    exit 1
  fi

  # (2) Launch a bad job to allow the next call to detect its failure
  echo "Launch a job that should fail (don't --wait)"
  BAD_JOB_PREVIOUS=$(run_dsub \
    --command 'sleep 5s && exit 1')

  # (3) This call to dsub should fail before submit
  echo "Launch a job that should fail (--after the previous)"
  if JOBID="$(run_dsub \
      --command 'echo "does not matter"' \
      --after "${BAD_JOB_PREVIOUS}" \
      --wait)"; then
    echo "Expected dsub to exit with error."
    exit 1
  elif [[ "${JOBID}" != "NO_JOB" ]]; then
    echo "dsub reported JOBID: '${JOBID}', expected 'NO_JOB'"
    exit 1
  fi

  # (4) Validate the end time for the failed job
  echo "Check that the failed job has a proper end-time set"
  DSTAT_OUTPUT=$(run_dstat --status '*' --jobs "${BAD_JOB_PREVIOUS}" --full)
  if ! util::dstat_yaml_job_has_valid_end_time "${DSTAT_OUTPUT}"; then
    echo "dstat output for ${BAD_JOB_PREVIOUS} does not include a valid end time."
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

fi

echo "SUCCESS"
