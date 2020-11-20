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

# Test the --retries dsub flag.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do retries setup
source "${SCRIPT_DIR}/retries_setup.sh"

readonly JOB_NAME="$(retries_setup::get_job_name "$(basename "${0}")")"

echo "Launch a job that should fail with --retries 1"
retries_setup::run_dsub "${JOB_NAME}" 1 false

echo

echo "Checking task metadata for job that should fail with --retries 1..."
retries_setup::check_job_attr "${JOB_NAME}" status "FAILURE FAILURE"
retries_setup::check_job_attr "${JOB_NAME}" task-attempt "2 1"

echo "SUCCESS"
