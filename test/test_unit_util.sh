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

# Define the script exit handler

function exit_handler() {
  # Grab the exit code
  local code="${?}"

  if [[ "${code}" -eq 0 ]]; then
    # Only clean-up the temp dir if all tests pass
    rm -rf "${TEST_TEMP}"

    echo
    echo "$(basename "${0}") exiting with SUCCESS"
  else
    echo
    echo "$(basename "${0}") exiting with FAILURE."

    echo
    echo "STDOUT:"
    cat "${TEST_STDOUT}"

    echo
    echo "STDERR:"
    cat "${TEST_STDERR}"
  fi

  exit "${code}"
}

# Define some standard routines for tests

function test_passed() {
  local test_name="${1}"
  echo "Test ${test_name}: PASSED"
}
readonly -f test_passed

function test_failed() {
  local test_name="${1}"
  echo "Test ${test_name}: FAILED"

  exit 1
}
readonly -f test_failed

#
# Generic routines for checking test output (stdout and stderr)
#

function get_stderr_value() {
  local value="${1}"

  python "${SCRIPT_DIR}"/get_json_value.py \
    "$(<"${TEST_STDERR}")" "${value}"
}
readonly -f get_stderr_value

function assert_err_value_equals() {
  local key="${1}"
  local value="${2}"

  local actual=$(get_stderr_value "${key}")
  if [[ "${actual}" != "${value}" ]]; then
    2>&1 echo "Assert: actual value for ${key}, ${actual}, does not match expected: ${value}"

    exit 1
  fi
}
readonly -f assert_err_value_equals

function assert_err_value_matches() {
  local key="${1}"
  local re="${2}"

  local actual=$(get_stderr_value "${key}")
  if ! echo "${actual}" | grep --quiet "${re}"; then
    2>&1 echo "Assert: value for ${key} does not match expected pattern:"
    2>&1 echo "EXPECTED pattern:"
    2>&1 echo "${re}"
    2>&1 echo "ACTUAL:"
    2>&1 echo "${actual}"

    exit 1
  fi
}
readonly -f assert_err_value_matches

function assert_output_contains() {
  local expected="${1}"

  if ! grep --quiet --fixed-strings "${expected}" "${TEST_STDOUT}"; then
    2>&1 echo "Assert: stdout does not contain expected output:"
    2>&1 echo "EXPECTED:"
    2>&1 echo "${expected}"
    2>&1 echo "ACTUAL:"
    2>&1 echo "$(<$"${TEST_STDOUT}")"

    exit 1
  fi
}
readonly -f assert_output_contains

function assert_err_contains() {
  local expected="${1}"

  if ! grep --quiet --fixed-strings "${expected}" "${TEST_STDERR}"; then
    2>&1 echo "Assert: stderr does not contain expected output:"
    2>&1 echo "EXPECTED:"
    2>&1 echo "${expected}"
    2>&1 echo "ACTUAL:"
    2>&1 echo "$(<$"${TEST_STDERR}")"

    exit 1
  fi
}
readonly -f assert_err_contains

function assert_output_empty() {
  [[ ! -s "${TEST_STDOUT}" ]] || "Assert: stdout is not empty"
}
readonly -f assert_output_empty

function assert_err_empty() {
  [[ ! -s "${TEST_STDERR}" ]] || "Assert: stderr is not empty"
}
readonly -f assert_err_empty

#
# Utility routines specific to pipelines structures dumped by
# running dsub with the --dryrun flag.
#

# assert_pipeline_label_equals
#
# Utility routine for checking that a pipeline label has a specific value.
function assert_pipeline_label_equals() {
  local job_idx="${1}"
  local label="${2}"
  local value="${3}"

  assert_err_value_equals \
    "[${job_idx}].pipelineArgs.labels.${label}" \
    "${value}"
}
readonly -f assert_pipeline_label_equals

# assert_pipeline_label_matches
#
# Utility routine for checking that a pipeline label matches a specific
# regular expression.
function assert_pipeline_label_matches() {
  local job_idx="${1}"
  local label="${2}"
  local re="${3}"

  assert_err_value_matches \
    "[${job_idx}].pipelineArgs.labels.${label}" \
    "${re}"
}
readonly -f assert_pipeline_label_matches

# assert_pipeline_environment_variable_equals
#
# Utility routine for checking a parameter value in the pipelineArgs.
function assert_pipeline_environment_variable_equals() {
  local job_idx="${1}"
  local var_name="${2}"
  local var_value="${3}"

  assert_err_value_equals \
    "[${job_idx}].pipelineArgs.inputs.${var_name}" \
    "${var_value}"
}
readonly -f assert_pipeline_environment_variable_equals

# assert_pipeline_input_parameter_equals
#
# Utility routine for checking both the input parameter declaration
# in the ephemeralPipeline as well as the parameter value in the pipelineArgs.
function assert_pipeline_input_parameter_equals() {
  local job_idx="${1}"
  local var_name="${2}"
  local docker_path="${3}"
  local remote_uri="${4}"

  assert_err_value_equals \
    "[${job_idx}].ephemeralPipeline.inputParameters.{name=\"${var_name}\"}.localCopy.path" \
    "${docker_path}"

  assert_err_value_equals \
    "[${job_idx}].pipelineArgs.inputs.${var_name}" \
    "${remote_uri}"
}
readonly -f assert_pipeline_input_parameter_equals

# assert_pipeline_output_parameter_equals
#
# Utility routine for checking both the output parameter declaration
# in the ephemeralPipeline as well as the parameter value in the pipelineArgs.
function assert_pipeline_output_parameter_equals() {
  local job_idx="${1}"
  local var_name="${2}"
  local docker_path="${3}"
  local remote_uri="${4}"

  assert_err_value_equals \
    "[${job_idx}].ephemeralPipeline.outputParameters.{name=\"${var_name}\"}.localCopy.path" \
    "${docker_path}"

  assert_err_value_equals \
    "[${job_idx}].pipelineArgs.outputs.${var_name}" \
    "${remote_uri}"
}
readonly -f assert_pipeline_output_parameter_equals

# assert_pipeline_job_id_matches
#
# Utility routine for checking that the generated "job-id" label matches
# the expected format.
function assert_pipeline_job_id_matches() {
  local job_idx="${1}"
  local name="${2}"

  # A job-id will look like: "script-nam--mbookman--161129-133400-96", 
  # Note that the script-name portion is limited to 10 characters
  assert_pipeline_label_matches "${job_idx}" "job-id" \
    "^${name:0:10}--${USER}--[0-9]\{6\}-[0-9]\{6\}-[0-9]\{2\}$"
}
readonly -f assert_pipeline_job_id_matches

