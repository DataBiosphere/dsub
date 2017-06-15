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
# This test use the default stock image (ubuntu:14.04).
#
# This test is designed to verify that:
#  * gsutil can be installed automatically if not present in the image.
#  * recursive inputs are copied from GCS to the VM recursively.
#  * non-recursive inputs are processed non-recursively.
#  * script-populated output directories are recursively copied.
#  * non-recursive output directories are processed non-recursively.
#
# This script will populate GCS with a two identical sets of directories and
# files, named both "deep" and "shallow".
# The "deep" GCS path will be copied recursively as input.
# The "shallow" GCS path will be copied non-recursively as input.
#
# Similarly the pipeline script will generate output locally to both
# a "deep" and "shallow" directory.
# The "deep" local path will be copied recursively as output.
# The "shallow" local path will be copied non-recursively as output.
#
# The structure of the data generated for the deep and shallow copying is:
#
#   file{1,2}.txt
#   dir_{1,2}/file{1,2}.txt
#   dir_{1,2}/dir_{a,b}/file{1,2}.txt

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"
trap "util::exit_handler ${TEST_TEMP}" EXIT

readonly FILE_CONTENTS="Test file contents"

readonly INPUT_ROOT="${TEST_TEMP}/inputs"
readonly INPUT_DEEP="${INPUT_ROOT}/deep"
readonly INPUT_SHALLOW="${INPUT_ROOT}/shallow"

echo "Setting up test inputs"

readonly DIR_LIST=(
  dir_1/dir_a
  dir_1/dir_b
  dir_2/dir_a
  dir_2/dir_b
)

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  mkdir -p "${TEST_TEMP}"

  echo "Setting up pipeline input..."

  for INPUT_DIR in "${INPUT_DEEP}" "${INPUT_SHALLOW}"; do
    mkdir -p "${INPUT_DIR}"

    for DIR in "${DIR_LIST[@]}"; do
      mkdir -p "${INPUT_DIR}/${DIR}"
    done

    DIRS=($(find "${INPUT_DIR}" -type d))
    for DIR in "${DIRS[@]}"; do
      echo "${FILE_CONTENTS}" > "${DIR}/file1.txt"
      echo "${FILE_CONTENTS}" > "${DIR}/file2.txt"
    done
  done

  gsutil -m rsync -r "${INPUT_ROOT}" "${INPUTS}/"

  echo "Launching pipeline..."

  run_dsub \
    --image "debian" \
    --script "${SCRIPT_DIR}/script_io_recursive.sh" \
    --env FILE_CONTENTS="${FILE_CONTENTS}" \
    --input INPUT_PATH_SHALLOW="${INPUTS}/shallow/*" \
    --input-recursive INPUT_PATH_DEEP="${INPUTS}/deep/" \
    --output OUTPUT_PATH_SHALLOW="${OUTPUTS}/shallow/*" \
    --output-recursive OUTPUT_PATH_DEEP="${OUTPUTS}/deep/" \
    --wait

fi

echo
echo "Checking output..."

# Check the results

readonly EXPECTED_FS_INPUT_ENTRIES=(
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/dir_a/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/dir_a/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/dir_b/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/dir_b/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_1/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/dir_a/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/dir_a/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/dir_b/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/dir_b/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/dir_2/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/deep/file2.txt
/mnt/data/input/"${DOCKER_INPUTS}"/shallow/file1.txt
/mnt/data/input/"${DOCKER_INPUTS}"/shallow/file2.txt
)

readonly EXPECTED_FS_OUTPUT_ENTRIES=(
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/dir_a/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/dir_a/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/dir_b/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/dir_b/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_1/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/dir_a/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/dir_a/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/dir_b/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/dir_b/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/dir_2/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/deep/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/dir_a/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/dir_a/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/dir_b/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/dir_b/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_1/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/dir_a/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/dir_a/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/dir_b/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/dir_b/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/dir_2/file2.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/file1.txt
/mnt/data/output/"${DOCKER_OUTPUTS}"/shallow/file2.txt
)

readonly EXPECTED_GCS_OUTPUT_ENTRIES=(
"${OUTPUTS}"/:
"${OUTPUTS}"/deep/:
"${OUTPUTS}"/deep/dir_1/:
"${OUTPUTS}"/deep/dir_1/dir_a/:
"${OUTPUTS}"/deep/dir_1/dir_a/file1.txt
"${OUTPUTS}"/deep/dir_1/dir_a/file2.txt
"${OUTPUTS}"/deep/dir_1/dir_b/:
"${OUTPUTS}"/deep/dir_1/dir_b/file1.txt
"${OUTPUTS}"/deep/dir_1/dir_b/file2.txt
"${OUTPUTS}"/deep/dir_1/file1.txt
"${OUTPUTS}"/deep/dir_1/file2.txt
"${OUTPUTS}"/deep/dir_2/:
"${OUTPUTS}"/deep/dir_2/dir_a/:
"${OUTPUTS}"/deep/dir_2/dir_a/file1.txt
"${OUTPUTS}"/deep/dir_2/dir_a/file2.txt
"${OUTPUTS}"/deep/dir_2/dir_b/:
"${OUTPUTS}"/deep/dir_2/dir_b/file1.txt
"${OUTPUTS}"/deep/dir_2/dir_b/file2.txt
"${OUTPUTS}"/deep/dir_2/file1.txt
"${OUTPUTS}"/deep/dir_2/file2.txt
"${OUTPUTS}"/deep/file1.txt
"${OUTPUTS}"/deep/file2.txt
"${OUTPUTS}"/shallow/:
"${OUTPUTS}"/shallow/file1.txt
"${OUTPUTS}"/shallow/file2.txt
)

# Verify in the stdout file that the expected directories were written
readonly RESULT=$(gsutil cat "${STDOUT_LOG}")

readonly FS_FIND_IN=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p' \
  | grep --fixed-strings /mnt/data/input/"${DOCKER_INPUTS}")

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
  | grep --fixed-strings /mnt/data/output/"${DOCKER_OUTPUTS}")

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
readonly GCS_FIND="$(gsutil ls -r "${OUTPUTS}" | grep -v '^ *$')"

for REC in "${EXPECTED_GCS_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${GCS_FIND}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Output does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${GCS_FIND}"
    exit 1
  fi
done

GCS_REC_COUNT=$(echo "${GCS_FIND}" | wc -l)
if [[ "${GCS_REC_COUNT}" -ne "${#EXPECTED_GCS_OUTPUT_ENTRIES[@]}" ]]; then
  2>&1 echo "Number of records in ${OUTPUTS} does not match expected"
  2>&1 echo "${GCS_REC_COUNT} != ${#EXPECTED_GCS_OUTPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "GCS output file list matches expected"

echo "SUCCESS"
