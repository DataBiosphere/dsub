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

# Test --input arguments that have wildcards.
#
# The purpose of this script is to demonstrate how to handle environment
# variables in which the value contains a wildcard, such as:
#
# INPUT_FILES=/mnt/data/input/gs/bucket/path/myfiles*.txt
#
# There are some small details to pay attention to:
#
# * Wrapping the variable expansion in double-quotes will suppress wildcard
#   expansion (which you don't want to suppress when listing files).
# * If there may be spaces in the path, you must double-quote the variable
#   expansion.
#
# In the simplest case (no spaces in the path), one can do the following:
#
# for FILE in ${MY_INPUT_PARAM}; do
#   <do stuff with FILE>
# done
#
# If there are spaces in the path, then things take some more work.
# - Break up the path into "path" and "filename" pieces.
# - Make sure to quote the "path" and don't quote the "filename" expansions.
# - The simple "for VAR in LIST..." syntax breaks down as LIST must be
#   tokenized by newlines rather than whitespace
#

# Simple path with no spaces

for FILE in ${INPUT_BASIC}; do
  FILE_PATH="$(dirname "${FILE}")"
  FILE_NAME="$(basename "${FILE}")"

  echo "FILE_PATH=${FILE_PATH}"
  echo "FILE_NAME=${FILE_NAME}"
done

# Path which could have spaces

readonly INPUT_FILES_PATH="$(dirname "${INPUT_WITH_SPACE}")"
readonly INPUT_FILES_PATTERN="$(basename "${INPUT_WITH_SPACE}")"

# shellcheck disable=SC2086
declare INPUT_FILE_LIST="$(ls -1 "${INPUT_FILES_PATH}"/${INPUT_FILES_PATTERN})"
# shellcheck enable=SC2086
IFS=$'\n' INPUT_FILE_LIST=(${INPUT_FILE_LIST})
readonly INPUT_FILE_LIST

for FILE in "${INPUT_FILE_LIST[@]}"; do
  FILE_PATH="$(dirname "${FILE}")"
  FILE_NAME="$(basename "${FILE}")"

  echo "FILE_PATH=${FILE_PATH}"
  echo "FILE_NAME=${FILE_NAME}"
done
