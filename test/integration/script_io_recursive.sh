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

# script_io_recursive.sh
#
# Script which helps test the "recursive output" support in dsub.
# This script will iterate over all variables named "OUTPUT_PATH_*"
# and will create a fixed set of subdirectories and files.
#
# Each file will contain the same text as specified by the FILE_CONTENTS
# environment variable.

set -o errexit
set -o nounset

readonly DIR_LIST=(
  dir_1/dir_a
  dir_1/dir_b
  dir_2/dir_a
  dir_2/dir_b
)

# Emit for debugging
echo "BEGIN: env"
env
echo "END: env"

# This test is set up to expect the output paths to be specified in variables
# that are named OUTPUT_PATH_DEEP and OUTPUT_PATH_SHALLOW.
#
# OUTPUT_PATH_DEEP should be a full directory reference (not ending in a slash).
# OUTPUT_PATH_SHALLOW should be a wildcard path .../gs/path/*.
#
# We handle both cases here, though not by explicit name; use bash indirect
# variable references to process all environment variables starting with
# "OUTPUT_PATH_"
for OUTPUT_PATH_VAR in ${!OUTPUT_PATH_*}; do
  OUTPUT_PATH_VAL=${!OUTPUT_PATH_VAR}
  echo "${OUTPUT_PATH_VAR} = ${OUTPUT_PATH_VAL}"

  # If the last component of the path contains a wildcard, then remove it.
  if [[ "$(basename "${OUTPUT_PATH_VAL}")" == *"*"* ]]; then
    OUTPUT_PATH_VAL="$(dirname "${OUTPUT_PATH_VAL}")"
  fi

  # If the path ends in a trailing slash, remove it.
  OUTPUT_PATH_VAL=${OUTPUT_PATH_VAL%/}

  for DIR in "${DIR_LIST[@]}"; do
    echo "Creating directory ${OUTPUT_PATH_VAL}/${DIR}"
    mkdir -p "${OUTPUT_PATH_VAL}/${DIR}"
  done

  DIRS=($(find "${OUTPUT_PATH_VAL}" -type d))
  for DIR in "${DIRS[@]}"; do
    echo "Populating ${DIR}"

    echo "${FILE_CONTENTS}" > "${DIR}/file1.txt"
    echo "${FILE_CONTENTS}" > "${DIR}/file2.txt"
  done
done

# Before exiting, emit the data directory details, such that the test
# can verify the local files written
echo "BEGIN: find"
find "${INPUT_PATH_SHALLOW%/*}" -type f
find "${INPUT_PATH_DEEP}" -type f
find "${OUTPUT_PATH_SHALLOW%/*}" -type f
find "${OUTPUT_PATH_DEEP}" -type f
echo "END: find"

