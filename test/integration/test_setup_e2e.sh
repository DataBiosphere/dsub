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
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}/logging
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

declare PROJECT_ID
if [[ -n "${YOUR_PROJECT:-}" ]]; then
  PROJECT_ID="${YOUR_PROJECT}"
else
  echo "Checking configured gcloud project"
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null)"
fi

if [[ -z "${PROJECT_ID}" ]]; then
  1>&2 echo "Your project ID could not be determined."
  1>&2 echo "Set the environment variable YOUR_PROJECT or run \"gcloud init\"."
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
if ! gcloud storage ls "gs://${DSUB_BUCKET}" 2>/dev/null; then
  1>&2 echo "Bucket does not exist (or we have no access): ${DSUB_BUCKET}"
  1>&2 echo "Create the bucket with \"gcloud storage buckets create\"."
  1>&2 echo "Current gcloud settings:"
  1>&2 echo "  account: $(gcloud config get-value account 2>/dev/null)"
  1>&2 echo "  project: $(gcloud config get-value project 2>/dev/null)"
  1>&2 echo "  pass_credentials_to_gsutil: $(gcloud config get-value pass_credentials_to_gsutil 2>/dev/null)"
  exit 1
fi

declare DSUB_BUCKET_REQUESTER_PAYS
if [[ -n "${YOUR_BUCKET_REQUESTER_PAYS:-}" ]]; then
  DSUB_BUCKET_REQUESTER_PAYS="${YOUR_BUCKET_REQUESTER_PAYS}"
else
  DSUB_BUCKET_REQUESTER_PAYS="dsub-test-requester-pays-public"
fi

# GPU-specific prerequisites (optional, only needed for GPU tests)
if [[ -n "${DOCKER_IMAGE:-}" ]]; then
  echo "  GAR image for GPU tests: ${DOCKER_IMAGE}"

  # Check if PET_SA_EMAIL is also set
  if [[ -z "${PET_SA_EMAIL:-}" ]]; then
    1>&2 echo "WARNING: DOCKER_IMAGE is set but PET_SA_EMAIL is not."
    1>&2 echo "GPU tests require both DOCKER_IMAGE and PET_SA_EMAIL to be set."
  else
    echo "  Service account for GPU tests: ${PET_SA_EMAIL}"

    # Validate that the service account can access the GAR image
    # echo "  Validating service account access to GAR image..."

    # # Extract the repository from the image path
    # # Format: REGION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG
    # GAR_REPO=$(echo "${DOCKER_IMAGE}" | sed -E 's|^([^/]+/[^/]+/[^/]+)/.*|\1|')

    # # Check if the service account has permission to pull from this repository
    # # We'll use gcloud artifacts docker images describe with impersonation
    # if ! gcloud artifacts docker images describe "${DOCKER_IMAGE}" \
    #      --impersonate-service-account="${PET_SA_EMAIL}" \
    #      --quiet 2>/dev/null; then
    #   1>&2 echo "WARNING: Service account ${PET_SA_EMAIL} may not have access to ${DOCKER_IMAGE}"
    #   1>&2 echo "Please ensure the service account has 'Artifact Registry Reader' role on the repository."
    #   1>&2 echo "You can grant access with:"
    #   1>&2 echo "  gcloud artifacts repositories add-iam-policy-binding REPO_NAME \\"
    #   1>&2 echo "    --location=LOCATION \\"
    #   1>&2 echo "    --member=serviceAccount:${PET_SA_EMAIL} \\"
    #   1>&2 echo "    --role=roles/artifactregistry.reader"
    # else
    #   echo "  ✓ Service account has access to GAR image"
    # fi
  fi
elif [[ -n "${PET_SA_EMAIL:-}" ]]; then
  echo "  Service account for GPU tests: ${PET_SA_EMAIL}"
  1>&2 echo "WARNING: PET_SA_EMAIL is set but DOCKER_IMAGE is not."
  1>&2 echo "GPU tests require both DOCKER_IMAGE and PET_SA_EMAIL to be set."
fi

# Set standard LOGGING, INPUTS, and OUTPUTS values
readonly TEST_GCS_ROOT="gs://${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}"
readonly TEST_GCS_DOCKER_ROOT="gs/${DSUB_BUCKET}/dsub/sh/${DSUB_PROVIDER}/${TEST_NAME}"
readonly TEST_LOCAL_ROOT="${TEST_TMP}"
readonly TEST_LOCAL_DOCKER_ROOT="file${TEST_LOCAL_ROOT}"


if [[ -n "${TASKS_FILE:-}" ]]; then
  # For task file tests, the logging path is a directory.
  readonly LOGGING="${TEST_GCS_ROOT}/logging"
else
  # For regular tests, the logging path is a named file.
  readonly LOGGING="${TEST_GCS_ROOT}/logging/${TEST_NAME}.log"
  readonly STDOUT_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stdout.log"
  readonly STDERR_LOG="$(dirname "${LOGGING}")/${TEST_NAME}-stderr.log"
fi

readonly INPUTS="${TEST_GCS_ROOT}/input"
readonly OUTPUTS="${TEST_GCS_ROOT}/output"
readonly DOCKER_GCS_INPUTS="${TEST_GCS_DOCKER_ROOT}/input"
readonly DOCKER_GCS_OUTPUTS="${TEST_GCS_DOCKER_ROOT}/output"
readonly LOCAL_INPUTS="${TEST_LOCAL_ROOT}/input"
readonly LOCAL_OUTPUTS="${TEST_LOCAL_ROOT}/output"
readonly DOCKER_LOCAL_INPUTS="${TEST_LOCAL_DOCKER_ROOT}/input"
readonly DOCKER_LOCAL_OUTPUTS="${TEST_LOCAL_DOCKER_ROOT}/output"

echo "Logging path: ${LOGGING}"
echo "Input path: ${INPUTS}"
echo "Output path: ${OUTPUTS}"

# For tests that exercise remote dsub parameters (like TSV file)
readonly DSUB_PARAMS="${TEST_GCS_ROOT}/params"

echo "  Checking if remote test files already exists"
if gcloud storage ls "${TEST_GCS_ROOT}/**" 2>/dev/null; then
  1>&2 echo "Test files exist: ${TEST_GCS_ROOT}"
  1>&2 echo "Remove contents:"
  1>&2 echo "  gcloud storage rm --recursive ${TEST_GCS_ROOT}/**"
  exit 1
fi

if [[ -n "${TASKS_FILE:-}" ]]; then
  # For a task file test, set up the task file from its template
  # This should really be a feature of dsub directly...
  echo "Setting up task file ${TASKS_FILE}"
  mkdir -p "$(dirname "${TASKS_FILE}")"
  if [[ -e "${TASKS_FILE_TMPL}" ]]; then
    cat "${TASKS_FILE_TMPL}" \
      | util::expand_tsv_fields \
      > "${TASKS_FILE}"
  fi
fi
