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

# Test for the --after and --wait flags

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

TGT_1="${OUTPUTS}/testfile_1.txt"
TGT_2="${OUTPUTS}/testfile_2.txt"


if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOBID=$(run_dsub \
    --image 'ubuntu' \
    --command 'sleep 5s; echo hi > $OUT' \
    --output OUT="${TGT_1}")

  run_dsub \
    --image 'ubuntu' \
    --command 'cat $IN > $OUT' \
    --input  IN="${TGT_1}" \
    --output OUT="${TGT_2}" \
    --after "${JOBID}" \
    --wait

fi

echo
echo "Checking output..."

readonly RESULT="$(gsutil cat "${TGT_2}")"
if [[ "${RESULT}" != "hi" ]]; then
  echo "Output file does not match expected"
  echo "Expected: hi"
  echo "Got: ${RESULT}"
  exit 1
fi

echo
echo "Output file matches expected."
echo "*****************************"

echo "SUCCESS"
