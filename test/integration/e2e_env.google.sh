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

# Test the google provider sets up the environment as expected.

# Do standard test setup
readonly SCRIPT_DIR="$(dirname "${0}")"
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly COMMAND='\
  echo "Working directory:"
  pwd

  echo "Data directory:"
  ls /mnt/data

  echo "Script directory:"
  ls /mnt/data/script

  echo "TMPDIR:"
  echo "${TMPDIR}"
'

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  "${DSUB}" \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --image "ubuntu" \
    --zones "us-central1-*" \
    --name "google_env.sh" \
    --command "${COMMAND}" \
    --wait

fi

echo
echo "Checking output..."

# Check the results
readonly RESULT_EXPECTED=$(cat <<EOF
Working directory:
/mnt/data/workingdir
Data directory:
lost+found
script
tmp
workingdir
Script directory:
google_env.sh
TMPDIR:
/mnt/data/tmp
EOF
)

readonly RESULT="$(gsutil cat "${STDOUT_LOG}")"
if ! diff <(echo "${RESULT_EXPECTED}") <(echo "${RESULT}"); then
  echo "Output file does not match expected"
  exit 1
fi

echo
echo "Output file matches expected:"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"

echo "SUCCESS"

