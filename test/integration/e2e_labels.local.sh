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

function check_label() {
  local LABEL_LINE="$1"
  # Expected: dstat's YAML output in ${DSTAT_OUTPUT}, with a section like:
  #
  # labels:
  #  label1: value1
  #  label2: value2
  #
  # and $LABEL_LINE would be e.g. "label2: value2"
  if ! echo "${DSTAT_OUTPUT}" | grep 'labels:' -A2 | grep -q "${LABEL_LINE}"; then
    echo "Label ${LABEL_LINE} not found in the dstat output!"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi
}

# Test labels.
#
# This test launches a single job and then verifies that dstat
# reflects the labels that were specified in dsub.

readonly SCRIPT_DIR="$(dirname "${0}")"

# This test is not sensitive to the output of the dsub job.
# Set the ALLOW_DIRTY_TESTS environment variable to 1 in your shell to
# run this test without first emptying the output and logging directories.
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Set up for running the tests
mkdir -p "${TEST_TMP}"

# Create a simple TSV file
readonly TASKS_FILE="${TEST_TMP}/${TEST_NAME}.tsv"
util::write_tsv_file "${TASKS_FILE}" \
' --label song
  cheryl
  milestones
'

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOBID="$(run_dsub \
    --label genre=jazz \
    --label subgenre=bebop \
    --command "echo 'hello world'")"

  echo "Checking dstat (full)..."

  if ! DSTAT_OUTPUT=$(run_dstat --status '*' --full --jobs "${JOBID}" 2>&1); then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  check_label 'genre: jazz'
  check_label 'subgenre: bebop'

  # As of this writing we cannot combine --labels from the command line
  # and task file, that's why we run two tests.

  echo "Launching pipeline..."

  JOBID="$(run_dsub \
    --tasks "${TASKS_FILE}" \
    --command "echo 'hello world'")"

  echo "Checking dstat (full)..."

  if ! DSTAT_OUTPUT=$(run_dstat --status '*' --full --jobs "${JOBID}" 2>&1); then
    echo "dstat exited with a non-zero exit code!"
    echo "Output:"
    echo "${DSTAT_OUTPUT}"
    exit 1
  fi

  check_label 'song: cheryl'
  check_label 'song: milestones'

  echo "SUCCESS"

fi


