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

# Basic end to end test, driven by a --tasks file.
#
# This test is designed to verify that --skip works.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

TEST_FILE_PATH_1="${OUTPUTS}/testfile_1.txt"
TEST_FILE_PATH_2="${OUTPUTS}/testfile_2.txt"
TEST_FILE_PATH_3="${OUTPUTS}/testfile_3.txt"

if gsutil ls "${TEST_FILE_PATH_1}" &> /dev/null; then
  echo "Unexpected: the output file '${TEST_FILE_PATH_1}' already exists."
  exit 1
fi

mkdir -p "${TEST_TMP}"
# TASKS_FILE is defined for us in test_setup.sh)
util::write_tsv_file "${TASKS_FILE}" \
"
  --output OUTPUT_PATH
  ${TEST_FILE_PATH_1}
"

echo "1. Should execute, write its output."
JOB_ID="$(
  run_dsub \
    --tasks "${TASKS_FILE}" \
    --command 'echo "hello world" > "${OUTPUT_PATH}"' \
    --skip \
    --wait)"

if ! gsutil ls "${TEST_FILE_PATH_1}" &> /dev/null; then
  echo "Unexpected: the output file '${TEST_FILE_PATH_1}' was not created."
  exit 1
fi

RESULT="$(gsutil cat "${TEST_FILE_PATH_1}")"
if [[ "${RESULT}" != "hello world" ]]; then
  echo "Output file does not match expected (from step 1)"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi

echo "2. Should skip because output already exists."
JOB_ID="$(
  run_dsub \
    --tasks "${TASKS_FILE}" \
    --command 'echo "hello again, world" > "${OUTPUT_PATH}"' \
    --skip \
    --wait)"

RESULT="$(gsutil cat "${TEST_FILE_PATH_1}")"
if [[ "${RESULT}" != "hello world" ]]; then
  echo "Output file does not match expected (from step 2)"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi

# Create a TSV file with two tasks
util::write_tsv_file "${TASKS_FILE}" \
"
  --output OUTPUT_PATH\t--env ROW
  ${TEST_FILE_PATH_1}\t1
  ${TEST_FILE_PATH_2}\t2
"

echo "3. One task should be skipped, the other run."
JOB_ID="$(
  run_dsub \
    --tasks "${TASKS_FILE}" \
    --command 'echo "hello again from row ${ROW}" > "${OUTPUT_PATH}"' \
    --skip \
    --wait)"

RESULT="$(gsutil cat "${TEST_FILE_PATH_1}")"
if [[ "${RESULT}" != "hello world" ]]; then
  echo "Output file does not match expected (from step 3)"
  echo "Expected: hello world"
  echo "Got: ${RESULT}"
  exit 1
fi
RESULT="$(gsutil cat "${TEST_FILE_PATH_2}")"
if [[ "${RESULT}" != "hello again from row 2" ]]; then
  echo "Output file does not match expected (from step 3)"
  echo "Expected: hello again from row 2"
  echo "Got: ${RESULT}"
  exit 1
fi

echo "SUCCESS"
