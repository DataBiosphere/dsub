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
# Intended to be sourced into a test via either
# test_setup_e2e.sh or test_setup_unit.sh.
#
# The code here will:
#
# * Ensure the DSUB_PROVIDER is set (default: local)
# * Set the TEST_NAME based on the name of the calling script.
# * Set the TEST_DIR to the directory the test file is in.
# * For task file tests, set TASKS_FILE and TASKS_FILE_TMPL.
# * Set the TEST_TMP variable for a temporary directory.
#
# * Provide functions run_dsub, run_dstat, run_ddel which will call a function
#   with DSUB_PROVIDER-specific default parameters set.

# If the DSUB_PROVIDER is not set, figure it out from the name of the script.
#   If the script name is <test>.<provider>.sh, pull out the provider.
#   If the script name is <test>.sh, use "local".
# If the DSUB_PROVIDER is set, make sure it is correct for a provider test.

readonly SCRIPT_NAME="$(basename "$0")"
readonly SCRIPT_DEFAULT_PROVIDER=$(
  if [[ "${SCRIPT_NAME}" == *.*.* ]]; then
    echo "${SCRIPT_NAME}" | awk -F . '{ print $(NF-1) }'
  fi
)
if [[ -z "${DSUB_PROVIDER:-}" ]]; then
  readonly DSUB_PROVIDER="${SCRIPT_DEFAULT_PROVIDER:-local}"
elif [[ -n "${SCRIPT_DEFAULT_PROVIDER}" ]]; then
  if [[ "${DSUB_PROVIDER:-}" != "${SCRIPT_DEFAULT_PROVIDER}" ]]; then
    2>&1 echo "DSUB_PROVIDER is '${DSUB_PROVIDER:-}' not '${SCRIPT_DEFAULT_PROVIDER}'"
    exit 1
  fi
fi

# Compute the name of the test from the calling script
readonly TEST_NAME="$(echo "${SCRIPT_NAME}" | \
  sed -e 's#e2e_\(.*\)\.sh#\1#' \
      -e 's#unit_\(.*\)\.sh#\1#')"

echo "Setting up test: ${TEST_NAME}"

readonly TEST_DIR="${SCRIPT_DIR}"

readonly TEST_TMP="${TEST_TMP:-/tmp/dub_test/sh/${DSUB_PROVIDER}/${TEST_NAME}}/tmp"

if [[ "${TEST_NAME}" == *_tasks ]]; then
  readonly TASKS_FILE_TMPL="${TEST_DIR}/${TASKS_FILE_TMPL_NAME:-${TEST_NAME}}.tsv.tmpl"
  readonly TASKS_FILE="${TEST_TMP}/${TEST_NAME}.tsv"
fi

# Functions for launching dsub
#
# Tests should generally just call run_dsub, run_dstat, or run_ddel,
# which will then invoke the provider-specific function.

# dsub

function run_dsub() {
  dsub_"${DSUB_PROVIDER}" "${@}"
}

function dsub_google() {
  dsub \
    --provider google \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "${ZONES:-us-central1-*}" \
    "${DISK_SIZE:+--disk-size ${DISK_SIZE}}" \
    "${BOOT_DISK_SIZE:+--boot-disk-size ${BOOT_DISK_SIZE}}" \
    "${@}"
}

function dsub_local() {
  dsub \
    --provider local \
    --logging "${LOGGING}" \
    "${@}"
}

function dsub_test-fails() {
  dsub \
    --provider test-fails \
    "${@}"
}

# dstat

function run_dstat() {
  dstat_"${DSUB_PROVIDER}" "${@}"
}

function dstat_google() {
  dstat \
    --provider google \
    --project "${PROJECT_ID}" \
    "${@}"
}

function dstat_local() {
  dstat \
    --provider local \
    "${@}"
}

# ddel

function run_ddel() {
  local provider=${DSUB_PROVIDER:-google}

  ddel_"${provider}" "${@}"
}

function ddel_google() {
  ddel \
    --provider google \
    --project "${PROJECT_ID}" \
    "${@}"
}

function ddel_local() {
  ddel \
    --provider local \
    "${@}"
}
