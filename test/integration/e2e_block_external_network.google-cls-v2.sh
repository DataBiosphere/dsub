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

# The user script runs "gcloud storage ls", so it needs an image with gcloud.
# Read dsub's own constant rather than duplicating the image reference here;
# that keeps this test from going stale when the constant is updated, and lets
# DSUB_CLOUD_SDK_IMAGE steer the test too.
readonly CLOUD_SDK_IMAGE="$(python3 -c \
  'from dsub.providers import google_utils; print(google_utils.CLOUD_SDK_IMAGE)')"
if [[ -z "${CLOUD_SDK_IMAGE}" ]]; then
  1>&2 echo "Could not read CLOUD_SDK_IMAGE from dsub.providers.google_utils."
  exit 1
fi
echo "Using image: ${CLOUD_SDK_IMAGE}"

echo "Launching pipeline..."

set +o errexit

# script_block_external_network.sh sets CLOUDSDK_STORAGE_MAX_RETRIES=0 when
# running "gcloud storage ls" below. Otherwise, gcloud storage will retry
# due to the network error.
JOB_ID="$(run_dsub \
  --image "${CLOUD_SDK_IMAGE}" \
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

# A blocked network reaches the log as one of several messages depending on the
# gcloud version in the image: older releases report a handled urllib3 "Max
# retries exceeded", while current ones surface an unhandled "gcloud crashed
# (ConnectionError)" traceback. Assert on the invariant -- gcloud could not
# reach the GCS endpoint -- rather than on one release's prose, so that a
# reworded gcloud error does not read as a dsub regression.
readonly GCLOUD_NETWORK_ERROR_RE='max retries exceeded|connectionerror|connection refused|failed to establish a new connection|could not resolve|name or service not known|network is unreachable'
# curl reports its failure on a single line, so keep the host in the pattern.
# Otherwise a gcloud network error elsewhere in the log could satisfy this
# check while curl's own error is missing.
readonly CURL_NETWORK_ERROR_RE='(could not resolve host|failed to connect to|resolving timed out.*) *:? *google\.com'

for STDERR_LOG_FILE in "${ATTEMPT_1_STDERR_LOG}" "${ATTEMPT_2_STDERR_LOG}" ; do
  RESULT="$(gcloud storage cat "${STDERR_LOG_FILE}")"
  if ! echo "${RESULT}" | grep -qi "storage.googleapis.com" \
      || ! echo "${RESULT}" | grep -qiE "${GCLOUD_NETWORK_ERROR_RE}"; then
    1>&2 echo "Network error from gcloud not found in the dsub stderr log!"
    1>&2 echo "${RESULT}"
    exit 1
  fi

  if ! echo "${RESULT}" | grep -qiE "${CURL_NETWORK_ERROR_RE}"; then
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

