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

# Basic environment variable test.
#
# This test use a stock image (ubuntu).
# No input files.
# No output files.
# The stdout file is checked for expected output.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  # VAL4 tests spaces in variable values
  run_dsub \
    --image "ubuntu" \
    --script "${SCRIPT_DIR}/script_env_test.sh" \
    --env VAR1="VAL1" VAR2="VAL2" VAR3="VAL3" \
    --env VAR4="VAL4 (four)" \
    --env VAR5="VAL5" \
    --wait

fi

echo
echo "Checking output..."

# Check the results
readonly RESULT_EXPECTED=$(cat <<EOF
VAR1=VAL1
VAR2=VAL2
VAR3=VAL3
VAR4=VAL4 (four)
VAR5=VAL5
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

