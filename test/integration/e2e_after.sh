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

# Test for the --after and --wait flags

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

TEST_FILE_PATH_1="${OUTPUTS}/testfile_1.txt"
TEST_FILE_PATH_2="${OUTPUTS}/testfile_2.txt"


if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  # (1) Launch a simple job that should succeed after a short wait
  JOBID=$(run_dsub \
    --command 'sleep 5s && echo "hello world" > "${OUT}"' \
    --output OUT="${TEST_FILE_PATH_1}")

  # (2) Wait for the previous job and then launch a new one that blocks
  # until exit
  run_dsub \
    --after "${JOBID}" \
    --command 'cat "${IN}" > "${OUT}"' \
    --input IN="${TEST_FILE_PATH_1}" \
    --output OUT="${TEST_FILE_PATH_2}" \
    --wait

  # (3) Launch a job to test command execution failure
  if run_dsub \
      --command 'exit 1' \
      --wait; then
    echo "Expected dsub to exit with error."
    exit 1
  fi

  # (4) Launch a bad job to allow the next call to detect its failure
  BAD_JOB_PREVIOUS=$(run_dsub \
    --command 'sleep 5s && exit 1')

  # (5) This call to dsub should fail before submit
  if run_dsub \
      --command 'echo "does not matter"' \
      --after "${BAD_JOB_PREVIOUS}" \
      --wait; then
    echo "Expected dsub to exit with error."
    exit 1
  fi

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
