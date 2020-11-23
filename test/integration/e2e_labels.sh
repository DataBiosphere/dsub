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
  if ! echo "${DSTAT_OUTPUT}" | grep 'labels:' -A10 | grep -q "${LABEL_LINE}"; then
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

source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly TASKS_FILE="${TEST_TMP}/${TEST_NAME}.tsv"

# Set up for running the tests
mkdir -p "${TEST_TMP}"

# Create a simple TSV file
util::write_tsv_file "${TASKS_FILE}" '
  --label item-number
  1
  2
'

echo "Launching pipeline..."

JOBID="$(run_dsub \
  --label batch=hello-world \
  --label item-number=1 \
  --command "echo 'hello world'; sleep 4m;")"

echo "Checking dstat (full)..."

if ! DSTAT_OUTPUT=$(run_dstat --status '*' --full --jobs "${JOBID}" 2>&1); then
  echo "dstat exited with a non-zero exit code!"
  echo "Output:"
  echo "${DSTAT_OUTPUT}"
  exit 1
fi

check_label "batch: hello-world"
check_label "item-number: '1'"

echo
echo "Running ddel against a non-matching label - should not kill pipeline."
run_ddel --jobs "${JOBID}" --label "batch=hello-world" --label "item-number=2"

echo
echo "Check that the job is not canceled."
if util::wait_for_canceled_status "${JOBID}"; then
  echo "ERROR: Operation is canceled, but ddel should not have canceled it."
  util::get_job_status "${JOBID}"
  exit 1
fi

echo
echo "Killing the pipeline"
run_ddel --jobs "${JOBID}" --label "batch=hello-world" --label "item-number=1"

if ! util::wait_for_canceled_status "${JOBID}"; then
  echo "dstat does not show the operation as canceled after wait."
  util::get_job_status "${JOBID}"
  exit 1
fi

# As of this writing we cannot combine --labels from the command line
# and task file, that's why we run two tests.

echo "Launching pipeline..."

JOBID="$(run_dsub \
  --tasks "${TASKS_FILE}" \
  --label batch=hello-world \
  --command "echo 'hello world'")"

echo "Checking dstat (full)..."

if ! DSTAT_OUTPUT=$(run_dstat --status '*' --full --jobs "${JOBID}" 2>&1); then
  echo "dstat exited with a non-zero exit code!"
  echo "Output:"
  echo "${DSTAT_OUTPUT}"
  exit 1
fi

check_label "batch: hello-world"
check_label "item-number: '1'"
check_label "item-number: '2'"

echo "SUCCESS"

