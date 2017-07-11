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

# Test --input arguments that have wildcards.
#
# The purpose of this test is to ensure that the user script gets the
# full path (including wildcard) as an environment variable and that
# we have a good demonstration of how to handle such environment variables.
# See script_input_wildcards.sh.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly FILE_CONTENTS="Test file contents"

readonly INPUT_ROOT="${TEST_TMP}/inputs"
readonly INPUT_BASIC="${INPUT_ROOT}/basic"
readonly INPUT_WITH_SPACE="${INPUT_ROOT}/with space"
readonly GS_INPUT_BASIC="${INPUTS}/basic/*"
readonly GS_INPUT_WITH_SPACE="${INPUTS}/with space/*"

function exit_handler() {
  local code="${?}"

  # Only cleanup on success
  if [[ "${code}" -eq 0 ]]; then
    rm -rf "${TEST_TMP}"
    gsutil -mq rm "${INPUTS}/**"
  fi

  return "${code}"
}
readonly -f exit_handler

trap "exit_handler" EXIT


if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  mkdir -p "${INPUT_ROOT}"

  echo "Setting up pipeline input..."

  for INPUT_DIR in "${INPUT_BASIC}" "${INPUT_WITH_SPACE}"; do
    mkdir -p "${INPUT_DIR}"

    for INDEX in {1..3}; do
      echo "${FILE_CONTENTS}" > "${INPUT_DIR}/file.${INDEX}.txt"
    done
  done

  gsutil -m rsync -r "${INPUT_ROOT}" "${INPUTS}/"

  echo "Launching pipeline..."

  run_dsub \
    --script "${SCRIPT_DIR}/script_input_wildcards.sh" \
    --input INPUT_BASIC="${GS_INPUT_BASIC}" \
    --input INPUT_WITH_SPACE="${GS_INPUT_WITH_SPACE}" \
    --vars-include-wildcards \
    --wait

fi

echo
echo "Checking output..."

# Check the results
readonly RESULT_EXPECTED=$(cat <<EOF
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/basic
FILE_NAME=file.1.txt
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/basic
FILE_NAME=file.2.txt
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/basic
FILE_NAME=file.3.txt
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/with space
FILE_NAME=file.1.txt
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/with space
FILE_NAME=file.2.txt
FILE_PATH=/mnt/data/input/${DOCKER_INPUTS}/with space
FILE_NAME=file.3.txt
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
