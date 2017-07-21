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

# test_setup.sh
#
# Intended to be sourced into a test.
# The code here will:
#
# * Set the following variables:
#    - DIR_LIST
#    - EXPECTED_REMOTE_OUTPUT_ENTRIES
#    - EXPECTED_FS_INPUT_ENTRIES
#    - EXPECTED_FS_OUTPUT_ENTRIES
# * Create the following functions:
#    - build_recursive_files
#    - setup_expected_fs_input_entries
#    - setup_expected_fs_output_entries
#    - setup_expected_remote_output_entries


DIR_LIST=(
  dir_1/dir_a
  dir_1/dir_b
  dir_2/dir_a
  dir_2/dir_b
)


# Passed the path of two directories for shallow, and deep input, this
# function will construct a directory structure and populate it with files
# having $FILE_CONTENTS as a payload.
function build_recursive_files() {
  local INPUT_DEEP_LOCAL="${1}"
  local INPUT_SHALLOW_LOCAL="${2}"

  mkdir -p "${INPUT_DEEP_LOCAL}"

  for INPUT_DIR in "${INPUT_DEEP_LOCAL}" "${INPUT_SHALLOW_LOCAL}"; do
    mkdir -p "${INPUT_DIR}"

    for DIR in "${DIR_LIST[@]}"; do
      mkdir -p "${INPUT_DIR}/${DIR}"
    done

    DIRS=($(find "${INPUT_DIR}" -type d))
    for DIR in "${DIRS[@]}"; do
      echo "${FILE_CONTENTS}" > "${DIR}/in_file1.txt"
      echo "${FILE_CONTENTS}" > "${DIR}/in_file2.txt"
      chmod o-rwx "${DIR}"/in_file*
    done
  done
}


function setup_expected_fs_input_entries() {
  # Strip the trailing slash if it was provided"
  local PREFIX="${1%/}"
  EXPECTED_FS_INPUT_ENTRIES=(
  /mnt/data/input/"${PREFIX}"/deep/dir_1/dir_a/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_1/dir_a/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_1/dir_b/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_1/dir_b/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_1/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_1/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/dir_a/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/dir_a/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/dir_b/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/dir_b/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/dir_2/in_file2.txt
  /mnt/data/input/"${PREFIX}"/deep/in_file1.txt
  /mnt/data/input/"${PREFIX}"/deep/in_file2.txt
  /mnt/data/input/"${PREFIX}"/shallow/in_file1.txt
  /mnt/data/input/"${PREFIX}"/shallow/in_file2.txt
  )
}


function setup_expected_fs_output_entries() {
  local PREFIX="${1%/}"
  EXPECTED_FS_OUTPUT_ENTRIES=(
  /mnt/data/output/"${PREFIX}"/deep/dir_1/dir_a/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_1/dir_a/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_1/dir_b/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_1/dir_b/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_1/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_1/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/dir_a/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/dir_a/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/dir_b/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/dir_b/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/dir_2/file2.txt
  /mnt/data/output/"${PREFIX}"/deep/file1.txt
  /mnt/data/output/"${PREFIX}"/deep/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/dir_a/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/dir_a/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/dir_b/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/dir_b/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_1/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/dir_a/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/dir_a/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/dir_b/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/dir_b/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/dir_2/file2.txt
  /mnt/data/output/"${PREFIX}"/shallow/file1.txt
  /mnt/data/output/"${PREFIX}"/shallow/file2.txt
  )
}

function setup_expected_remote_output_entries() {
  local PREFIX="${1%/}"
  EXPECTED_REMOTE_OUTPUT_ENTRIES=(
  "${PREFIX}"
  "${PREFIX}"/deep
  "${PREFIX}"/deep/dir_1
  "${PREFIX}"/deep/dir_1/dir_a
  "${PREFIX}"/deep/dir_1/dir_a/file1.txt
  "${PREFIX}"/deep/dir_1/dir_a/file2.txt
  "${PREFIX}"/deep/dir_1/dir_b
  "${PREFIX}"/deep/dir_1/dir_b/file1.txt
  "${PREFIX}"/deep/dir_1/dir_b/file2.txt
  "${PREFIX}"/deep/dir_1/file1.txt
  "${PREFIX}"/deep/dir_1/file2.txt
  "${PREFIX}"/deep/dir_2
  "${PREFIX}"/deep/dir_2/dir_a
  "${PREFIX}"/deep/dir_2/dir_a/file1.txt
  "${PREFIX}"/deep/dir_2/dir_a/file2.txt
  "${PREFIX}"/deep/dir_2/dir_b
  "${PREFIX}"/deep/dir_2/dir_b/file1.txt
  "${PREFIX}"/deep/dir_2/dir_b/file2.txt
  "${PREFIX}"/deep/dir_2/file1.txt
  "${PREFIX}"/deep/dir_2/file2.txt
  "${PREFIX}"/deep/file1.txt
  "${PREFIX}"/deep/file2.txt
  "${PREFIX}"/shallow
  "${PREFIX}"/shallow/file1.txt
  "${PREFIX}"/shallow/file2.txt
  )
}
