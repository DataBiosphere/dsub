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

# Basic end to end test, without --tasks file.
#
# This test is designed to verify that --skip works.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

TEST_FILE_PATH_1="${OUTPUTS}/testfile_1.txt"
TEST_FILE_PATH_2="${OUTPUTS}/testfile_2.txt"

echo "hello world" | gsutil cp - "${TEST_FILE_PATH_1}"

if gsutil ls "${TEST_FILE_PATH_2}" &> /dev/null; then
  echo "Unexpected: the output file '${TEST_FILE_PATH_1}' already exists."
  exit 1
fi

echo "1. non-task single job: skip (output present)"
JOB_ID="$(
  run_dsub \
    --output OUTPUT_PATH="${TEST_FILE_PATH_1}" \
    --command 'echo "hello from the job" > "${OUTPUT_PATH}"' \
    --skip \
    --wait)"

RESULT="$(gsutil cat "${TEST_FILE_PATH_1}")"
if [[ "${RESULT}" != "hello world" ]]; then
  echo "Output file does not match expected (from step 4)"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi

echo "2. non-task single job: do not skip (output not present)"
JOB_ID="$(
  run_dsub \
    --output OUTPUT_PATH="${TEST_FILE_PATH_2}" \
    --command 'echo "hello from the job" > "${OUTPUT_PATH}"' \
    --skip \
    --wait)"

RESULT="$(gsutil cat "${TEST_FILE_PATH_2}")"
if [[ "${RESULT}" != "hello from the job" ]]; then
  echo "Output file does not match expected (from step 2)"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi

echo "SUCCESS"
