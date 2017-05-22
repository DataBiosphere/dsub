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

# unit_test_job_metadata.sh
#
# Simple unit tests to verify the labels that get set in the pipeline
# for a tasks job, such as "job-name", "user-id", "job-id", and "task-id".

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the label + tasks test

function run_dsub() {
  local tasks="${1}"

  "${DSUB}" \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "${ZONE}" \
    --script "${SCRIPT}" \
    --tasks "${tasks}" \
    --dry-run \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f run_dsub

# Define tests

function test_default_name() {
  local subtest="${FUNCNAME[0]}"

  local tsv_file="${TEST_TEMP}/${subtest}.tsv"

  # Create a simple TSV file
  util::write_tsv_file "${tsv_file}" \
'
--env SAMPLE_ID
na12878
na12879
'

  if run_dsub "${tsv_file}"; then

    # Check that the output contains expected labels
    #   "labels": {
    #     "job-id": "dummy--mbookman--161129-145520-09",
    #     "job-name": "dummy",
    #     "task-id": "task-1",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "dummy"
    assert_pipeline_label_equals 0 "job-name" "dummy"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" "task-1"

    assert_pipeline_job_id_matches 1 "dummy"
    assert_pipeline_label_equals 1 "job-name" "dummy"
    assert_pipeline_label_equals 1 "user-id" "${USER}"
    assert_pipeline_label_equals 1 "task-id" "task-2"

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_default_name

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TEMP}"

echo
test_default_name
