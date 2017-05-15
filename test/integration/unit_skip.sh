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

set -o errexit
set -o nounset

# Test the --skip feature.
#
# It checks:
# --output FILE
# --output VARNAME=FILE
# --output-recursive VARNAME=FILE
#
# It checks that we skip when the file's there, and also that we do not
# skip when the file isn't there.
#
# This test uses the test-fails provider so that it can detect whether
# a job is scheduled without having to actually schedule on the cloud.
# Despite using this provider, dsub will only fail when it tries to
# submit a job, not in the cases where it skips submission.


readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# create pre-existing output
EXISTING="${OUTPUTS}/exists"
EXISTING_2="${OUTPUTS}/exists_too"
EXISTING_PATTERN_1="${OUTPUTS}/exist*"
EXISTING_PATTERN_2="${OUTPUTS}/ex*ts"
NEWFILE="${OUTPUTS}/newfile"
OUT_FOLDER_2="${OUTPUTS}/newfolder"
echo "test output" | gsutil cp - "${EXISTING}"
echo "test output" | gsutil cp - "${EXISTING_2}"


echo "Job 1 ..."

# Skip because --output file exists.
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 1"' \
  --output "${EXISTING}" \
  --skip)

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 1 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 2 ..."

# Skip because --output file exists (VAR=PATH syntax)
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 2" > ${OUTPUT_FILE}' \
  --output OUTPUT_FILE="${EXISTING}" \
  --skip \
  --wait)

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 2 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 3 ..."

# Skip because --output file matches pattern
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 3"' \
  --output OUTPUT_FILES="${EXISTING_PATTERN_1}" \
  --skip \
  --wait)

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 3 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 4 ..."

# Skip because --output file matches pattern
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 4"' \
  --output OUTPUT_FILES="${EXISTING_PATTERN_2}" \
  --skip \
  --wait)

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 4 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 5 ..."

# Skip existing file in --output-recursive
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 5" > ${OUTPUTS_PATH}/job_5' \
  --output-recursive OUTPUTS_PATH="${OUTPUTS}" \
  --skip \
  --wait)

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 5 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 6 (should run)..."

# Do not skip because --output file isn't there
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 6" > ${OUTPUT_FILE}' \
  --output OUTPUT_FILE="${NEWFILE}" \
  --skip \
  --wait || echo "ran")

if [[ "${SKIP_JOBID}" = "NO_JOB" ]]; then
  echo "Job 6 was skipped, shouldn't have been."
  echo "Expected: not NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 7 (should run)..."

# Do not skip because --output-recursive folder is empty
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 7" > ${OUTPUT_PATH}/job_7' \
  --output-recursive OUTPUT_PATH="${OUT_FOLDER_2}" \
  --skip \
  --wait || echo "ran")

if [[ "${SKIP_JOBID}" = "NO_JOB" ]]; then
  echo "Job 7 was skipped, shouldn't have been."
  echo "Expected: not NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo "Job 8 ..."

# Skip because both --output files exist
SKIP_JOBID=$("${DSUB}" \
  --provider test-fails \
  --command 'echo "Job 8" > ${OUTPUT_FILE_1}' \
  --output OUTPUT_FILE_1="${EXISTING}" \
  --output OUTPUT_FILE_2="${EXISTING_2}" \
  --skip \
  --wait || echo "ran")

if [[ "${SKIP_JOBID}" != "NO_JOB" ]]; then
  echo "Job 8 wasn't skipped, should have been."
  echo "Expected: NO_JOB"
  echo "Actual:   ${SKIP_JOBID}"
  exit 1
fi

echo
echo "--skip worked fine."
echo "*****************************"
echo "SUCCESS"
