#!/bin/bash

# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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

# For google-v2, user-ids and job names are used as labels.
# Test that --user and --name works properly when it has underscores
# and capital letters.

# Do standard test setup
readonly SCRIPT_DIR="$(dirname "${0}")"
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  readonly RANDOM_STRING="$(cat /dev/urandom | env LC_CTYPE=C tr -cd 'a-z0-9' \
    | head -c 8)"
  readonly USER_NAME="UNDERSCORE_USER"
  readonly JOB_NAME="UNDERSCORE_JOB_${RANDOM_STRING}"

  # (1) Launch a simple job with a user_id and name containing
  # an underscore and caps
  echo "Launch a job (and don't --wait)..."
  JOB_ID="$(run_dsub \
    --command 'sleep 5s' \
    --user "${USER_NAME}" \
    --name "${JOB_NAME}")"

  sleep 5s

  # (2) Validate dstat can find the job by job id and username
  echo "Check that the job can be found with user"
  DSTAT_OUTPUT="$(run_dstat \
    --status '*' \
    --jobs "${JOB_ID}" \
    --users "${USER_NAME}")"
  if [[ -z "${DSTAT_OUTPUT}" ]]; then
    echo "ERROR: dstat output for ${JOB_ID} not found."
    exit 1
  fi

  # (3) Validate dstat can find the job by job name and username
  echo "Check that the job can be found with name"
  DSTAT_OUTPUT="$(run_dstat \
    --status '*' \
    --names "${JOB_NAME}" \
    --users "${USER_NAME}")"
  if [[ -z "${DSTAT_OUTPUT}" ]]; then
    echo "ERROR: dstat output for ${JOB_ID} not found."
    exit 1
  fi

  echo "dstat output was not empty."
  echo "Success!"
fi
