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

# test_setup_e2e.sh
#
# Intended to be sourced into a test.
# The code here will source test_setup.sh, and
#
# * Automatically determine PROJECT_ID
# * Automatically pick up a bucket name for tests.
#
# * Automatically set environment variables:
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}/logging/
#     (task file tests)
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}/logging/${TEST_NAME}.log
#     (non-task file tests)
#   * INPUTS=gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}/input
#   * OUTPUTS=gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}/output
#
# * Check if LOGGING, INPUTS, and OUTPUTS are empty.
# * For task file tests, generate the file from TASKS_FILE_TMPL.

source "${SCRIPT_DIR}/test_util.sh"
source "${SCRIPT_DIR}/test_setup.sh"

echo "Checking that required environment values are set:"

declare DSUB_PROVIDER=${DSUB_PROVIDER:-google}

declare PROJECT_ID
if [[ -n "${YOUR_PROJECT:-}" ]]; then
  PROJECT_ID="${YOUR_PROJECT}"
else
  echo "Checking configured gcloud project"
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
fi

if [[ -z "${PROJECT_ID}" ]]; then
  2>&1 echo "Your project ID could not be determined."
  2>&1 echo "Set the environment variable YOUR_PROJECT or run \"gcloud init\"."
  exit 1
fi

echo "  Project ID detected as: ${PROJECT_ID}"

declare DSUB_BUCKET
if [[ -n "${YOUR_BUCKET:-}" ]]; then
  DSUB_BUCKET="${YOUR_BUCKET}"
else
  DSUB_BUCKET="${USER}-dsub-test"
fi

echo "  Bucket detected as: ${DSUB_BUCKET}"

echo "  Checking if bucket exists"
if ! gsutil ls "gs://${DSUB_BUCKET}" 2>/dev/null; then
  2>&1 echo "Bucket does not exist (or we have no access): ${DSUB_BUCKET}"
  2>&1 echo "Create the bucket with \"gsutil mb\"."
  2>&1 echo "Current gcloud settings:"
  2>&1 echo "  account: $(gcloud config get-value account 2>/dev/null)"
  2>&1 echo "  project: $(gcloud config get-value project 2>/dev/null)"
  2>&1 echo "  pass_credentials_to_gsutil: $(gcloud config get-value pass_credentials_to_gsutil 2>/dev/null)"
  exit 1
fi

# Set standard LOGGING, INPUTS, and OUTPUTS values
readonly TEST_REMOTE_ROOT="gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}"
readonly TEST_DOCKER_ROOT="gs/${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}"

if [[ -n "${TASKS_FILE:-}" ]]; then
  # For task file tests, the logging path is a directory.
  # Eventually each job should have its own sub-directory,
  # and named logging files but we need to add dsub support for that.
  readonly LOGGING="${TEST_REMOTE_ROOT}/${TEST_NAME}/logging"
else
  # For regular tests, the logging path is a named file.
  readonly LOGGING="${TEST_REMOTE_ROOT}/logging/${TEST_NAME}.log"
  readonly STDOUT_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stdout.log"
  readonly STDERR_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stderr.log"
fi
readonly INPUTS="${TEST_REMOTE_ROOT}/input"
readonly OUTPUTS="${TEST_REMOTE_ROOT}/output"
readonly DOCKER_INPUTS="${TEST_DOCKER_ROOT}/input"
readonly DOCKER_OUTPUTS="${TEST_DOCKER_ROOT}/output"

echo "Logging path: ${LOGGING}"
echo "Input path: ${INPUTS}"
echo "Output path: ${OUTPUTS}"

# For tests that exercise remote dsub parameters (like TSV file)
readonly DSUB_PARAMS="gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/params"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "  Checking if remote test files already exists"
  if gsutil ls "${TEST_REMOTE_ROOT}/**" 2>/dev/null; then
    2>&1 echo "Test files exist: ${TEST_REMOTE_ROOT}"
    2>&1 echo "Remove contents:"
    2>&1 echo "  gsutil -m rm ${TEST_REMOTE_ROOT}/**"
    exit 1
  fi

fi

if [[ -n "${TASKS_FILE:-}" ]]; then
  # For a task file test, set up the task file from its template
  # This should really be a feature of dsub directly...
  echo "Setting up task file ${TASKS_FILE}"
  cat "${TASKS_FILE_TMPL}" \
    | util::expand_tsv_fields \
    > "${TASKS_FILE}"
fi

# Functions for launching dsub
#
# Tests should generally just call "run_dsub" which will then invoke
# the provider-specific function.

function run_dsub() {
  local provider=${DSUB_PROVIDER:-google}

  dsub_"${provider}" "${@}"
}

function dsub_google() {
  "${DSUB}" \
    --provider google \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "us-central1-*" \
    "${DISK_SIZE:+--disk-size ${DISK_SIZE}}" \
    "${BOOT_DISK_SIZE:+--boot-disk-size ${BOOT_DISK_SIZE}}" \
    "${@}"
}

function dsub_local() {
  "${DSUB}" \
    --provider local \
    --logging "${LOGGING}" \
    "${@}"
}
