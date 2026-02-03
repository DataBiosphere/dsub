#!/bin/bash

# Copyright 2021 Verily Life Sciences Inc. All Rights Reserved.
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

# Basic test of using the --block-external-network flag
# No input files.
# No output files.
# The stderr log file is checked for expected errors due to no network.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

echo "Launching pipeline..."

set +o errexit

# Note: gcloud storage commands will retry due to network errors.
# This test validates that the job fails when network access is blocked.
JOB_ID="$(run_dsub \
  --image 'gcr.io/google.com/cloudsdktool/cloud-sdk:327.0.0-slim' \
  --block-external-network \
  --script "${SCRIPT_DIR}/script_block_external_network.sh" \
  --retries 1 \
  --wait)"
if [[ $? -eq 0 ]]; then
  1>&2 echo "dsub did not report the failure as it should have."
  exit 1
fi
set -o errexit

echo
echo "Checking stderr of both attempts..."

# Check the results
readonly ATTEMPT_1_STDERR_LOG="$(dirname "${LOGGING}")/${TEST_NAME}.1-stderr.log"
readonly ATTEMPT_2_STDERR_LOG="$(dirname "${LOGGING}")/${TEST_NAME}.2-stderr.log"

for STDERR_LOG_FILE in "${ATTEMPT_1_STDERR_LOG}" "${ATTEMPT_2_STDERR_LOG}" ; do
  RESULT="$(gcloud storage cat "${STDERR_LOG_FILE}")"
  if ! echo "${RESULT}" | grep -qi "Unable to find the server at storage.googleapis.com"; then
    1>&2 echo "Network error from gcloud storage not found in the dsub stderr log!"
    1>&2 echo "${RESULT}"
    exit 1
  fi

  if ! echo "${RESULT}" | grep -qi "Could not resolve host: google.com"; then
    1>&2 echo "Network error from curl not found in the dsub stderr log!"
    1>&2 echo "${RESULT}"
    exit 1
  fi
done

echo
echo "Checking dstat output..."
ATTEMPT_1_DSTAT_OUTPUT=$(run_dstat --attempts 1 --status 'FAILURE' --full --jobs "${JOB_ID}" 2>&1);
ATTEMPT_2_DSTAT_OUTPUT=$(run_dstat --attempts 2 --status 'FAILURE' --full --jobs "${JOB_ID}" 2>&1);
for DSTAT_OUTPUT in "${ATTEMPT_1_DSTAT_OUTPUT}" "${ATTEMPT_1_DSTAT_OUTPUT}" ; do
  if ! echo "${DSTAT_OUTPUT}" | grep -qi "block-external-network: true"; then
    1>&2 echo "block-external-network not found in dstat output!"
    1>&2 echo "${DSTAT_OUTPUT}"
    exit 1
  fi
done

echo
echo "stderr log contains the expected errors."
echo "dstat output contains the expected block-external-network flag."
echo "SUCCESS"

