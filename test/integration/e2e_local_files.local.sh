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

# Test recursive copying features of dsub.
#
# This test is designed to verify that:
#  * The local runner can;
#     - accept local inputs
#     - write to a local output
#     - save logs to a local directory
#  * Non-recursive and recursive I/O should work for the local
#    runner no differently than the remote runner.
#  * Non-recursive and recursive I/O will create their output destinations.
#
# For a detailed description of what this test does, see e2e_io_recursive.sh.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"
# Extra test setup for recursive test. Defines several variables and
# brings in helper functions.
source "${SCRIPT_DIR}/test_setup_io_recursive.sh"

trap "util::exit_handler ${TEST_TMP}" EXIT

readonly FILE_CONTENTS="Test file contents"

# Sets up local directories where files will be written prior to pushing
# to the remote directory.
readonly INPUT_DEEP="${LOCAL_INPUTS}/deep"
readonly INPUT_SHALLOW="${LOCAL_INPUTS}/shallow"

# Output setup
readonly OUTPUT_DEEP="${LOCAL_OUTPUTS}/deep"
readonly OUTPUT_SHALLOW="${LOCAL_OUTPUTS}/shallow"
readonly LOCAL_LOGGING="${TEST_TMP}/output_logs"

echo "Setting up test inputs"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Setting up pipeline input..."
  build_recursive_files "${INPUT_DEEP}" "${INPUT_SHALLOW}"

  echo "Launching pipeline..."

  run_dsub \
    --image "google/cloud-sdk:latest" \
    --script "${SCRIPT_DIR}/script_io_recursive.sh" \
    --env FILE_CONTENTS="${FILE_CONTENTS}" \
    --input INPUT_PATH_SHALLOW="${INPUT_SHALLOW}/*" \
    --input-recursive INPUT_PATH_DEEP="${INPUT_DEEP}/" \
    --output OUTPUT_PATH_SHALLOW="${OUTPUT_SHALLOW}/*" \
    --output-recursive OUTPUT_PATH_DEEP="${OUTPUT_DEEP}/" \
    --wait

fi

echo
echo "Checking output..."

# Setup variables used in result checking.
setup_expected_fs_input_entries "${DOCKER_LOCAL_INPUTS}"
setup_expected_fs_output_entries "${DOCKER_LOCAL_OUTPUTS}"
setup_expected_remote_output_entries "${LOCAL_OUTPUTS}"

# Verify in the stdout file that the expected directories were written
readonly RESULT=$(gsutil cat "${STDOUT_LOG}")

readonly FS_FIND_IN=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p' \
  | grep --fixed-strings "${DOCKER_LOCAL_INPUTS}")

for REC in "${EXPECTED_FS_INPUT_ENTRIES[@]}"; do
  if ! echo "${FS_FIND_IN}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Input does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${FS_FIND_IN}"
    exit 1
  fi
done

readonly FS_REC_IN_COUNT=$(echo "${FS_FIND_IN}" | wc -l)
if [[ "${FS_REC_IN_COUNT}" -ne "${#EXPECTED_FS_INPUT_ENTRIES[@]}" ]]; then
  2>&1 echo "Number of records in /mnt/data/input does not match expected"
  2>&1 echo "${FS_REC_IN_COUNT} != ${#EXPECTED_FS_INPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "On-disk input file list matches expected"


readonly FS_FIND_OUT=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p' \
  | grep --fixed-strings "${DOCKER_LOCAL_OUTPUTS}")

for REC in "${EXPECTED_FS_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${FS_FIND_OUT}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Output does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${FS_FIND_OUT}"
    exit 1
  fi
done

readonly FS_REC_OUT_COUNT=$(echo "${FS_FIND_OUT}" | wc -l)
if [[ "${FS_REC_OUT_COUNT}" -ne "${#EXPECTED_FS_OUTPUT_ENTRIES[@]}" ]]; then
  2>&1 echo "Number of records in /mnt/data/output does not match expected"
  2>&1 echo "${FS_REC_OUT_COUNT} != ${#EXPECTED_FS_OUTPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "On-disk output file list matches expected"

# Verify in GCS that the DEEP directory is deep and the SHALLOW directory
# is shallow.
readonly LOCAL_FIND="$(find "${LOCAL_OUTPUTS}" | grep -v '^ *$')"

for REC in "${EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${LOCAL_FIND}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Output does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${LOCAL_FIND}"
    exit 1
  fi
done

GCS_REC_COUNT=$(echo "${LOCAL_FIND}" | wc -l)
if [[ "${GCS_REC_COUNT}" -ne "${#EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}" ]]; then
  2>&1 echo "Number of records in ${OUTPUTS} does not match expected"
  2>&1 echo "${GCS_REC_COUNT} != ${#EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "Local output file list matches expected"

echo "SUCCESS"
