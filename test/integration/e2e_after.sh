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

# Test the --after and --wait flags when dsub jobs succeed

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

TEST_FILE_PATH_1="${OUTPUTS}/testfile_1.txt"
TEST_FILE_PATH_2="${OUTPUTS}/testfile_2.txt"

# (1) Launch a simple job that should succeed after a short wait
echo "Launch a job (and don't --wait)..."
JOB_ID=$(run_dsub \
  --command 'sleep 5s && echo "hello world" > "${OUT}"' \
  --output OUT="${TEST_FILE_PATH_1}")

# (2) Wait for the previous job and then launch a new one that blocks
# until exit
echo "Launch a job (--after the previous, and then --wait)..."
run_dsub \
  --after "${JOB_ID}" \
  --command 'cat "${IN}" > "${OUT}"' \
  --input IN="${TEST_FILE_PATH_1}" \
  --output OUT="${TEST_FILE_PATH_2}" \
  --wait

# (3) Validate the end time for the failed job
echo "Check that the success job has a proper end-time set"
DSTAT_OUTPUT=$(run_dstat --status '*' --jobs "${JOB_ID}" --full)
if ! util::dstat_yaml_job_has_valid_end_time "${DSTAT_OUTPUT}"; then
  echo "dstat output for ${JOB_ID} does not include a valid end time."
  echo "${DSTAT_OUTPUT}"
  exit 1
fi

echo
echo "Checking output..."

readonly RESULT="$(gsutil cat "${TEST_FILE_PATH_2}")"
if [[ "${RESULT}" != "hello world" ]]; then
  echo "Output file does not match expected"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi

echo
echo "Output file matches expected:"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"

echo "SUCCESS"
