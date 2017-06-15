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
# * Set the TEST_NAME based on the name of the calling script.
# * Set variables for DSUB and TEST_DIR
# * For task file tests, set TASKS_FILE and TASKS_FILE_TMPL.
# * Set the TEST_TEMP variable for a temporary directory.

# Compute the name of the test from the calling script
readonly TEST_NAME="$(basename "${0}" | \
  sed -e 's#e2e_\(.*\)\.sh#\1#' \
      -e 's#unit_\(.*\)\.sh#\1#')"

echo "Setting up test: ${TEST_NAME}"

# dsub should be in the PATH, installed by setuptools (in a virtualenv)
readonly DSUB="dsub"

readonly TEST_DIR="${SCRIPT_DIR}"

if [[ "${TEST_NAME}" == *_tasks ]]; then
  readonly TASKS_FILE_TMPL="${TEST_DIR}/${TASKS_FILE_TMPL_NAME:-${TEST_NAME}}.tsv.tmpl"
  readonly TASKS_FILE="${TEST_DIR}/${TEST_NAME}.tsv"
fi

readonly TEST_TEMP=${TEST_DIR}/_tmp
