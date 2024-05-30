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
#   Special-case the google-v2 tests to be runnable for google-cls-v2
#   and google-batch.

readonly SCRIPT_NAME="$(basename "$0")"
readonly SCRIPT_DEFAULT_PROVIDER=$(
  if [[ "${SCRIPT_NAME}" == *.*.* ]]; then
    echo "${SCRIPT_NAME}" | awk -F . '{ print $(NF-1) }'
  fi
)
if [[ -z "${DSUB_PROVIDER:-}" ]]; then
  readonly DSUB_PROVIDER="${SCRIPT_DEFAULT_PROVIDER:-local}"
elif [[ -n "${SCRIPT_DEFAULT_PROVIDER}" ]]; then
  if [[ "${DSUB_PROVIDER}" == "google-cls-v2" ]] && \
     [[ "${SCRIPT_DEFAULT_PROVIDER}" == "google-v2" ]]; then
     echo "Running google-v2 e2e/unit tests with provider google-cls-v2"
  elif [[ "${DSUB_PROVIDER}" == "google-batch" ]] && \
     [[ "${SCRIPT_DEFAULT_PROVIDER}" == "google-v2" ]]; then
     echo "Running google-v2 e2e/unit tests with provider google-batch"
  elif [[ "${DSUB_PROVIDER}" != "${SCRIPT_DEFAULT_PROVIDER}" ]]; then
    1>&2 echo "DSUB_PROVIDER is '${DSUB_PROVIDER:-}' not '${SCRIPT_DEFAULT_PROVIDER}'"
    exit 1
  fi
fi

# Compute the name of the test from the calling script
readonly TEST_NAME="$(echo "${SCRIPT_NAME}" | \
  sed -e 's#e2e_\(.*\)\.sh#\1#' \
      -e 's#unit_\(.*\)\.sh#\1#')"

echo "Setting up test: ${TEST_NAME}"

readonly TEST_DIR="${SCRIPT_DIR}"

readonly TEST_TMP="${TEST_TMP:-/tmp/dsub-test/sh/${DSUB_PROVIDER}/${TEST_NAME}}/tmp"

if [[ "${TEST_NAME}" == *_tasks ]]; then
  readonly TASKS_FILE_TMPL="${TEST_DIR}/${TASKS_FILE_TMPL_NAME:-${TEST_NAME}}.tsv.tmpl"
  readonly TASKS_FILE="${TEST_TMP}/${TEST_NAME}.tsv"
fi

# Generate an id for tests to use that is reasonably likely to be unique
# (datestamp + 8 random characters).
export TEST_TOKEN="${TEST_TOKEN:-"$(printf "%s_%s" \
  "$(date +'%Y%m%d_%H%M%S')" \
  "$(cat /dev/urandom | env LC_CTYPE=C tr -cd 'a-z0-9' | head -c 8)")"}"

# Functions for launching dsub
#
# Tests should generally just call run_dsub, run_dstat, or run_ddel,
# which will then invoke the provider-specific function.

# dsub

function run_dsub() {
  dsub_"${DSUB_PROVIDER}" "${@}"
}

function dsub_google-batch() {
  dsub \
    --provider google-batch \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    "${@}"
}

function dsub_google-cls-v2() {
  local location="${LOCATION:-}"
  local zones="${ZONES:-}"
  local regions="${REGIONS:-}"

  dsub \
    --provider google-cls-v2 \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    ${regions:+--regions "${regions}"} \
    ${zones:+--zones "${zones}"} \
    "${@}"
}

function dsub_google-v2() {
  local zones="${ZONES:-}"
  local regions="${REGIONS:-}"
  if [[ -z "${regions}" ]] && [[ -z "${zones}" ]]; then
    regions="us-central1"
  fi

  dsub \
    --provider google-v2 \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    ${regions:+--regions "${regions}"} \
    ${zones:+--zones "${zones}"} \
    "${@}"
}

function dsub_local() {
  dsub \
    --provider local \
    --logging "${LOGGING_OVERRIDE:-${LOGGING}}" \
    "${@}"
}

function dsub_test-fails() {
  dsub \
    --provider test-fails \
    "${@}"
}

# dstat

function run_dstat_age() {
  # Call dstat, allowing the caller to set a "--age"
  local age="${1}"
  shift

  dstat_"${DSUB_PROVIDER}" --age "${age}" "${@}"
}

function run_dstat() {
  # Call dstat and automatically add "--age 45m".
  # This speeds up tests and helps avoid dstat calls that return jobs
  # from other test runs.
  # If a test takes longer than 45 minutes, then we should fix the test.
  run_dstat_age "45m" "${@}"
}

function dstat_google-batch() {
  local location="${LOCATION:-}"

  dstat \
    --provider google-batch \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    "${@}"
}

function dstat_google-cls-v2() {
  local location="${LOCATION:-}"

  dstat \
    --provider google-cls-v2 \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    "${@}"
}

function dstat_google-v2() {
  dstat \
    --provider google-v2 \
    --project "${PROJECT_ID}" \
    "${@}"
}

function dstat_local() {
  dstat \
    --provider local \
    "${@}"
}

# ddel

function run_ddel_age() {
  # Call ddel, allowing the caller to set a "--age"
  local age="${1}"
  shift

  ddel_"${DSUB_PROVIDER}" --age "${age}" "${@}"
}

function run_ddel() {
  # Call ddel and automatically add "--age 45m".
  # This speeds up tests and helps avoid ddel calls that return jobs
  # from other test runs.
  # If a test takes longer than 45 minutes, then we should fix the test.
  run_ddel_age "45m" "${@}"
}

function ddel_google-cls-v2() {
  local location="${LOCATION:-}"

  ddel \
    --provider google-cls-v2 \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    "${@}"
}

function ddel_google-v2() {
  ddel \
    --provider google-v2 \
    --project "${PROJECT_ID}" \
    "${@}"
}

function ddel_google-batch() {
  local location="${LOCATION:-}"

  ddel \
    --provider google-batch \
    --project "${PROJECT_ID}" \
    ${location:+--location "${location}"} \
    "${@}"
}

function ddel_local() {
  ddel \
    --provider local \
    "${@}"
}
