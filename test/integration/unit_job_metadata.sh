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
# such as "job-name", "user-id", "job-id", and "task-id".

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

# Define a utility routine for running the IO test

function run_dsub_with_script() {
  local name="${1}"

  "${DSUB}" \
    --dry-run \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "${ZONE}" \
    --script "${SCRIPT}" \
    --name "${name}" \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f run_dsub_with_script

function run_dsub_with_command() {
  local command="${1}"
  local name="${2:-}"

  "${DSUB}" \
    --dry-run \
    --project "${PROJECT_ID}" \
    --logging "${LOGGING}" \
    --zones "${ZONE}" \
    --name "${name}" \
    --command "${command}" \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"
}
readonly -f run_dsub_with_command

# Define tests

function test_default_name_from_script() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub_with_script ""; then

    # Check that the output contains expected labels:
    #   "labels": {
    #     "job-id": "dummy--mbookman--161129-133400-96",
    #     "job-name": "dummy",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "dummy"
    assert_pipeline_label_equals 0 "job-name" "dummy"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" ""

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_default_name_from_script

function test_explicit_name_override_script() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub_with_script "my-job"; then

    # Check that the output contains expected labels
    #   "labels": {
    #     "job-id": "my-job--mbookman--161129-133655-41",
    #     "job-name": "my-job",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "my-job"
    assert_pipeline_label_equals 0 "job-name" "my-job"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" ""


    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_explicit_name_override_script

function test_default_name_from_command() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub_with_command \
    "/in/my/docker/analysis.py"; then

    # Check that the output contains expected labels:
    #   "labels": {
    #     "job-id": "analysis--mbookman--161129-133400-96",
    #     "job-name": "analysis",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "analysis"
    assert_pipeline_label_equals 0 "job-name" "analysis"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" ""

    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_default_name_from_command

function test_explicit_name_override_command() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub_with_command \
    "/in/my/docker/analysis.py"\
    "my-job"; then

    # Check that the output contains expected labels
    #   "labels": {
    #     "job-id": "my-job--mbookman--161129-133655-41",
    #     "job-name": "my-job",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "my-job"
    assert_pipeline_label_equals 0 "job-name" "my-job"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" ""


    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_explicit_name_override_command

function test_long_name() {
  local subtest="${FUNCNAME[0]}"

  if run_dsub_with_script "my-job-which-has-a-long-name"; then

    # Check that the output contains expected labels
    #   "labels": {
    #     "job-id": "my-job--mbookman--161129-133655-41",
    #     "job-name": "my-job",
    #     "user-id": "mbookman"
    #   }

    assert_pipeline_job_id_matches 0 "my-job-which-has-a-long-name"
    assert_pipeline_label_equals 0 "job-name" "my-job-which-has-a-long-name"
    assert_pipeline_label_equals 0 "user-id" "${USER}"
    assert_pipeline_label_equals 0 "task-id" ""


    test_passed "${subtest}"
  else
    test_failed "${subtest}"
  fi
}
readonly -f test_long_name

# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TEMP}"

echo
test_default_name_from_script
test_explicit_name_override_script

echo
test_default_name_from_command
test_explicit_name_override_command

echo
test_long_name
