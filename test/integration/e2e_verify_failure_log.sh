#!/bin/bash

# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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

# Test that even when a job fails early, a useful status message is produced.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Run the job
if JOB_ID=$(run_dsub \
             --unique-job-id \
             --image gcr.io/no.such.image \
             --command 'echo "Test"' \
             --wait); then
  echo "ERROR: Job that should fail completed successfully!"
  exit 1
fi

readonly STATUS_MESSAGE="$(util::get_job_status_detail "${JOB_ID}")"

if [[ ${STATUS_MESSAGE} == *"no.such.image"* ]]; then
  echo
  echo "SUCCESS: dstat output for failed job mentions image problem."
  echo
else
  echo
  echo "ERROR: dstat output for failed job doesn't mention image problem."
  echo "${dstat_output}"
  echo
  exit 1
fi
