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
# Extra test setup for recursive test. Defines several variables and
# brings in helper functions.
source "${SCRIPT_DIR}/test_setup_io_recursive.sh"

trap "util::exit_handler ${TEST_TMP}" EXIT

readonly FILE_CONTENTS="Test file contents"

# Sets up local directories where files will be written prior to pushing
# to the remote directory.
readonly INPUT_DEEP="${LOCAL_INPUTS}/deep"
readonly INPUT_SHALLOW="${LOCAL_INPUTS}/shallow"


echo "Setting up test inputs"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Setting up pipeline input..."
  build_recursive_files "${INPUT_DEEP}" "${INPUT_SHALLOW}"

  gsutil -m rsync -r "${LOCAL_INPUTS}" "${INPUTS}/"

  echo "Launching pipeline..."

  JOB_ID="$(run_dsub \
    --image "google/cloud-sdk:latest" \
    --script "${SCRIPT_DIR}/script_io_recursive.sh" \
    --env FILE_CONTENTS="${FILE_CONTENTS}" \
    --input INPUT_PATH_SHALLOW="${INPUTS}/shallow/*" \
    --input-recursive INPUT_PATH_DEEP="${INPUTS}/deep/" \
    --output OUTPUT_PATH_SHALLOW="${OUTPUTS}/shallow/*" \
    --output-recursive OUTPUT_PATH_DEEP="${OUTPUTS}/deep/" \
    --wait)"
fi

echo
echo "Checking output..."

# Setup variables used in result checking.
setup_expected_fs_input_entries "${DOCKER_GCS_INPUTS}"
setup_expected_fs_output_entries "${DOCKER_GCS_OUTPUTS}"
setup_expected_remote_output_entries "${OUTPUTS}"

# Verify in the stdout file that the expected directories were written
readonly RESULT=$(gsutil cat "${STDOUT_LOG}")

readonly FS_FIND_IN=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p' \
  | grep --fixed-strings /mnt/data/input/"${DOCKER_GCS_INPUTS}")

for REC in "${EXPECTED_FS_INPUT_ENTRIES[@]}"; do
  if ! echo "${FS_FIND_IN}" | grep --quiet --fixed-strings "${REC}"; then
    1>&2 echo "Input does not match expected"
    1>&2 echo "Did not find ${REC} in:"
    1>&2 echo "${FS_FIND_IN}"
    exit 1
  fi
done

readonly FS_REC_IN_COUNT=$(echo "${FS_FIND_IN}" | wc -l)
if [[ "${FS_REC_IN_COUNT}" -ne "${#EXPECTED_FS_INPUT_ENTRIES[@]}" ]]; then
  1>&2 echo "Number of records in /mnt/data/input does not match expected"
  1>&2 echo "${FS_REC_IN_COUNT} != ${#EXPECTED_FS_INPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "On-disk input file list matches expected"

readonly FS_FIND_OUT=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p' \
  | grep --fixed-strings "${DOCKER_GCS_OUTPUTS}")

for REC in "${EXPECTED_FS_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${FS_FIND_OUT}" | grep --quiet --fixed-strings "${REC}"; then
    1>&2 echo "Output does not match expected"
    1>&2 echo "Did not find ${REC} in:"
    1>&2 echo "${FS_FIND_OUT}"
    exit 1
  fi
done

readonly FS_REC_OUT_COUNT=$(echo "${FS_FIND_OUT}" | wc -l)
if [[ "${FS_REC_OUT_COUNT}" -ne "${#EXPECTED_FS_OUTPUT_ENTRIES[@]}" ]]; then
  1>&2 echo "Number of records in /mnt/data/output does not match expected"
  1>&2 echo "${FS_REC_OUT_COUNT} != ${#EXPECTED_FS_OUTPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "On-disk output file list matches expected"

# Verify in GCS that the DEEP directory is deep and the SHALLOW directory
# is shallow. Gsutil prints directories with a trailing "/:" marker that is
# stripped using sed in order to match the output format of the `find` utility.
readonly GCS_FIND="$(gsutil ls -r "${OUTPUTS}" \
                     | grep -v '^ *$' \
                     | sed -e 's#/:$##')"

for REC in "${EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${GCS_FIND}" | grep --quiet --fixed-strings "${REC}"; then
    1>&2 echo "Output does not match expected"
    1>&2 echo "Did not find ${REC} in:"
    1>&2 echo "${GCS_FIND}"
    exit 1
  fi
done

GCS_REC_COUNT=$(echo "${GCS_FIND}" | wc -l)
if [[ "${GCS_REC_COUNT}" -ne "${#EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}" ]]; then
  1>&2 echo "Number of records in ${OUTPUTS} does not match expected"
  1>&2 echo "${GCS_REC_COUNT} != ${#EXPECTED_REMOTE_OUTPUT_ENTRIES[@]}"
  exit 1
fi

echo
echo "GCS output file list matches expected"

# Verify dstat
if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 && "${DSUB_PROVIDER}" != "google" ]]; then
  if ! DSTAT_OUTPUT="$(run_dstat --status '*' --full --jobs "${JOB_ID}")"; then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi
  util::dstat_yaml_assert_field_equal "${DSTAT_OUTPUT}" "[0].inputs" "{'INPUT_PATH_SHALLOW': '${INPUTS}/shallow/*'}"
  util::dstat_yaml_assert_field_equal "${DSTAT_OUTPUT}" "[0].outputs" "{'OUTPUT_PATH_SHALLOW': '${OUTPUTS}/shallow/*'}"
  util::dstat_yaml_assert_field_equal "${DSTAT_OUTPUT}" "[0].input-recursives" "{'INPUT_PATH_DEEP': '${INPUTS}/deep/'}"
  util::dstat_yaml_assert_field_equal "${DSTAT_OUTPUT}" "[0].output-recursives" "{'OUTPUT_PATH_DEEP': '${OUTPUTS}/deep/'}"
fi
echo "SUCCESS"
