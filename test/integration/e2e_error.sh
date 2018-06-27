#!/bin/bash

# Copyright 2017 Google Inc. All Rights Reserved.
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

# Test that we detect failures

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  if run_dsub \
    --image 'ubuntu' \
    --name 'e2e-error' \
    --command 'idontknowhowtounix' \
    --wait; then
    echo "dsub did not report the failure as it should have."
    exit 1
  fi

  DSTAT_OUTPUT=$(run_dstat --status '*' --names e2e-error --full)

  declare -a EXPECTED_EVENTS
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    EXPECTED_EVENTS=(start pulling-image localizing-files running-docker fail)
  elif [[ "${DSUB_PROVIDER}" == "local" ]]; then
    # The local provider has slightly different events in this error case
    EXPECTED_EVENTS=(start pulling-image localizing-files running-docker delocalizing-files fail)
  else
    # TODO: Update once events are implemented for google-v2
    EXPECTED_EVENTS=(TODO)
  fi

  for ((i = 0 ; i < "${#EXPECTED_EVENTS[@]}"; i++)); do
    EXPECTED="${EXPECTED_EVENTS[i]}"
    ACTUAL="$(python "${SCRIPT_DIR}"/get_data_value.py "yaml" "${DSTAT_OUTPUT}" "[0].events.[${i}].name")"
    if [[ ${ACTUAL} != ${EXPECTED} ]]; then
      echo "Job e2e-error has incorrect event (${i}): ${ACTUAL} should be: ${EXPECTED}"
      exit 1
    fi
  done
fi

echo "SUCCESS"

