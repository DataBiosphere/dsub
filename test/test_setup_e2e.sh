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
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/logging/
#     (table tests)
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/logging/${TEST_NAME}.log
#     (non-table tests)
#   * INPUTS=gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/input
#   * OUTPUTS=gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/output
#
# * Check if LOGGING, INPUTS, and OUTPUTS are empty.
# * For table tests, generate the file from TABLE_FILE_TMPL.

source "${SCRIPT_DIR}/test_util.sh"
source "${SCRIPT_DIR}/test_setup.sh"

echo "Checking that required environment values are set:"

declare PROJECT_ID
if [[ -n "${YOUR_PROJECT:-}" ]]; then
  PROJECT_ID="${YOUR_PROJECT}"
else
  echo "Checking configured gcloud project"
  PROJECT_ID="$(gcloud config list core/project --format='value(core.project)')"
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
  2>&1 echo "Bucket does not exist: ${DSUB_BUCKET}"
  2>&1 echo "Create the bucket with \"gsutil mb\"."
  exit 1
fi

# Set standard LOGGING, INPUTS, and OUTPUTS values
if [[ -n "${TABLE_FILE:-}" ]]; then
  # For table-based tests, the logging path is a directory.
  # Eventually each job should have its own sub-directory,
  # and named logging files but we need to add dsub support for that.
  readonly LOGGING="gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/logging/"
else
  # For regular tests, the logging path is a named file.
  readonly LOGGING="gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/logging/${TEST_NAME}.log"
  readonly STDOUT_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stdout.log"
  readonly STDERR_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stderr.log"
fi
readonly INPUTS="gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/input"
readonly OUTPUTS="gs://${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/output"
readonly DOCKER_INPUTS="gs/${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/input"
readonly DOCKER_OUTPUTS="gs/${DSUB_BUCKET}/dsub/sh/${TEST_NAME}/output"

echo "Logging path: ${LOGGING}"
echo "Input path: ${INPUTS}"
echo "Output path: ${OUTPUTS}"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "  Checking if logging files already exists"
  if gsutil ls "${LOGGING}" 2>/dev/null; then
    2>&1 echo "Logging files exist: ${LOGGING}"
    2>&1 echo "Remove contents:"
    2>&1 echo "  gsutil -m rm $(dirname "${LOGGING}")/**"
    exit 1
  fi

  echo "  Checking if input path already exists"
  if gsutil ls "${INPUTS}" 2>/dev/null; then
    2>&1 echo "Input path exists: ${INPUTS}"
    2>&1 echo "Remove contents:"
    2>&1 echo "  gsutil -m rm ${INPUTS}/**"
    exit 1
  fi

  echo "  Checking if output path already exists"
  if gsutil ls "${OUTPUTS}" 2>/dev/null; then
    2>&1 echo "Output path exists: ${OUTPUTS}"
    2>&1 echo "Remove contents:"
    2>&1 echo "  gsutil -m rm ${OUTPUTS}/**"
    exit 1
  fi

fi

if [[ -n "${TABLE_FILE:-}" ]]; then
  # For a table test, set up the table file from its template
  # This should really be a feature of dsub directly...
  echo "Setting up table file ${TABLE_FILE}"
  cat "${TABLE_FILE_TMPL}" \
    | util::expand_tsv_fields \
    > "${TABLE_FILE}"
fi
