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
# No input files.
# No output files.
# The stdout file is checked for expected output.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly LOGGING_OVERRIDE="${LOGGING}.log"
readonly NUM_TASKS=3

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  # Set up for running the tests
  mkdir -p "${TEST_TMP}"

  # Create a simple TSV which tests blank values in the first, last, and
  # a middle column.
  # Also include 0 to make sure it is passed through correctly as
  # a string ("0") and not dropped as int (0).
  util::write_tsv_file "${TASKS_FILE}" '
  --env TASK_VAR1\t--env TASK_VAR2\t--env TASK_VAR_ZERO\t--env TASK_VAR3
  \tVAL2_TASK1\t0\tVAL3_TASK1
  VAL1_TASK2\t\t0\tVAL3_TASK2
  VAL1_TASK3\tVAL2_TASK3\t0\t
  '

  echo "Launching pipeline..."

  # VAL4 tests spaces in variable values
  # VAR6 tests empty variable value
  run_dsub \
    --image "ubuntu" \
    --script "${SCRIPT_DIR}/script_env_test.sh" \
    --env VAR1="VAL1" VAR2="VAL2" VAR3="VAL3" \
    --env VAR4="VAL4 (four)" \
    --env VAR5="VAL5" \
    --env VAR6= \
    --env VAR_ZERO=0 \
    --tasks "${TASKS_FILE}" \
    --wait

fi

echo
echo "Checking output..."

# Check the results
for ((TASK_ID=1; TASK_ID <= NUM_TASKS; TASK_ID++)); do
  for ((VAL_ID=1; VAL_ID <= 3; VAL_ID++)); do
    if [[ "${VAL_ID}" == "${TASK_ID}" ]]; then
      eval TASK_VAR${VAL_ID}=""
    else
      eval TASK_VAR${VAL_ID}="VAL${VAL_ID}_TASK${TASK_ID}"
    fi
  done
  RESULT_EXPECTED="$(
    printf -- '
      TASK_VAR1=%s
      TASK_VAR2=%s
      TASK_VAR3=%s
      TASK_VAR_ZERO=0
      VAR1=VAL1
      VAR2=VAL2
      VAR3=VAL3
      VAR4=VAL4 (four)
      VAR5=VAL5
      VAR6=
      VAR_ZERO=0
      ' "${TASK_VAR1}" "${TASK_VAR2}" "${TASK_VAR3}" | \
    grep -v '^$' | \
    sed -e 's#^ *##'
  )"

  RESULT="$(gsutil cat "${LOGGING}.${TASK_ID}-stdout.log")"
  if ! diff <(echo "${RESULT_EXPECTED}") <(echo "${RESULT}"); then
    echo "Output file does not match expected"
    exit 1
  fi

  echo
  echo "Output file matches expected:"
  echo "*****************************"
  echo "${RESULT}"
  echo "*****************************"
done

echo "SUCCESS"
