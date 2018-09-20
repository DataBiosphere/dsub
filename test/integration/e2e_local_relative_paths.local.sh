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

# Test copying input files to and output files from local directories.
# The specific purpose of this test is to exercise the case where the
# local input and outputs are children of the current working directory.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Set the current working directory to the local test root
mkdir -p "${TEST_LOCAL_ROOT}"
cd "${TEST_LOCAL_ROOT}"

# Set the paths that we are passing to dsub as relative paths:
#   INPUT_FILE=inputs/relative/test.txt
#   OUTPUT_FILE=outputs/relative/test.txt

readonly INPUT_PATH_RELATIVE="$(
  python -c "import os; print(os.path.relpath('"${LOCAL_INPUTS}/relative"'));")"
readonly OUTPUT_PATH_RELATIVE="$(
  python -c "import os; print(os.path.relpath('"${LOCAL_OUTPUTS}/relative"'));")"

readonly INPUT_TEST_FILE="${INPUT_PATH_RELATIVE}/test.txt"
readonly OUTPUT_TEST_FILE="${OUTPUT_PATH_RELATIVE}/test.txt"

# Set up the input file
mkdir -p "$(dirname "${INPUT_TEST_FILE}")"
echo "This is only a test" > "${INPUT_TEST_FILE}"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  run_dsub \
    --input INPUT_FILE="${INPUT_TEST_FILE}" \
    --output OUTPUT_FILE="${OUTPUT_TEST_FILE}" \
    --command 'cp "${INPUT_FILE}" "${OUTPUT_FILE}"' \
    --wait

fi

if ! diff "${INPUT_TEST_FILE}" "${OUTPUT_TEST_FILE}"; then
  echo "Output file does not match expected"
  exit 1
fi

echo
echo "Output file matches expected:"
echo "*****************************"
cat "${OUTPUT_TEST_FILE}"
echo "*****************************"

echo "SUCCESS"
