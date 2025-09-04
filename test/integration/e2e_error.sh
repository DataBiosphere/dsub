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

echo "Launching pipeline..."

set +o errexit
JOB_ID="$(run_dsub \
  --name 'e2e-error' \
  --command 'idontknowhowtounix' \
  --wait)"
if [[ $? -eq 0 ]]; then
  echo "dsub did not report the failure as it should have."
  exit 1
fi
set -o errexit

DSTAT_OUTPUT=$(run_dstat --status '*' --jobs "${JOB_ID}" --full)

declare -a EXPECTED_EVENTS
if [[ "${DSUB_PROVIDER}" == "local" ]]; then
  # The local provider has slightly different events in this error case
  EXPECTED_EVENTS=(start pulling-image localizing-files running-docker delocalizing-files fail)
elif [[ "${DSUB_PROVIDER}" == "google-batch" ]]; then
  EXPECTED_EVENTS=(scheduled start fail)
else
  EXPECTED_EVENTS=(start pulling-image localizing-files running-docker fail)
fi
util::dstat_out_assert_equal_events "${DSTAT_OUTPUT}" "[0].events" "${EXPECTED_EVENTS[@]}"

echo "SUCCESS"

