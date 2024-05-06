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

# Global variables for running tests in parallel

# Generate an id for tests to use that is reasonably likely to be unique
# (datestamp + 8 random characters).
export TEST_TOKEN="${TEST_TOKEN:-"$(printf "%s_%s" \
  "$(date +'%Y%m%d_%H%M%S')" \
  "$(cat /dev/urandom | env LC_CTYPE=C tr -cd 'a-z0-9' | head -c 8)")"}"

# Set up a local directory for tests to write intermediate files
readonly TEST_OUTPUT_DIR="/tmp/dsub-test/${TEST_TOKEN}"
readonly TEST_TIMES_FILE="${TEST_OUTPUT_DIR}"/timing.txt

# We allow for the integration tests to run concurrently.
# By default, we run all of them at the same time, but MAX_CONCURRENCY
# can be toggled to rate limit that if necessary.
#
# For example, on systems with low GCE quota, the tests would run, but our
# timing of each test would be off (as they would sit in the Pipelines API
# queue waiting for GCE quota).
#
# Also possible that launching many local provider tests concurrently on a
# machine with limited memory could lead to failures.

# Set to a positive integer for specific concurrency
readonly MAX_CONCURRENCY=-1

# While integration tests are running in the background, we record the process
# pid and the test-specific output directory. The output directory stores
# the stderr+stdout (output.txt) along with the test exit code (exit.txt)
declare TEST_PIDS=()
declare TEST_DIRS=()

# Functions for running integration tests

function get_test_output_dir() {
  local provider="${1}"
  local test_file="${2}"

  echo "${TEST_OUTPUT_DIR}/${provider}/$(basename "${test_file}")"
}

function run_integration_test() {
  local provider="${1}"
  local test="${2}"

  # Create a label "test (provider)" for the start/end messages
  local test_label="$(basename "${test}") (${provider})"

  # Set up a directory for test-specific output
  local test_dir="$(get_test_output_dir "${provider}" "${test}")"
  local test_output_file="${test_dir}/output.txt"
  local test_exit_file="${test_dir}/exit.txt"

  mkdir -p "${test_dir}"

  # Execute the command in a background shell and:
  #   * send all output to output.txt
  #   * capture the exit code to exit.txt
  (
  start_test "${test_label}"

  export TEST_TMP="${test_dir}"
  export DSUB_PROVIDER="${provider}"

  # Disable errexit so we can record the test exit code
  set +o errexit
  if [[ "${test}" == *.py ]]; then
    # Execute the test as a module, such as "python -m test.e2e_env_list"
    python -m "test.integration.$(basename "${test%.py}")"
  else
    "${test}"
  fi
  echo "$?" > "${test_exit_file}"
  set -o errexit

  end_test "${test_label}"
  ) &>"${test_output_file}" &

  # Record the pid and test directory
  TEST_PIDS+=($!)
  TEST_DIRS+=("${test_dir}")
}
readonly -f run_integration_test

# check_wait_for_integration_tests
#
# If we have NOT reached maximum concurrency, then just return.
# If we have reached maximum concurrency then:
#   * wait for tests to complete
#   * emit the results of the tests
#
# This function takes an optional argument that if non-empty forces the
# the function to wait on any running tests.
function check_wait_for_integration_tests() {
  local force_wait="${1:-}"

  if [[ -z "${force_wait}" ]]; then
    if [[ "${MAX_CONCURRENCY}" -lt 0 ]]; then
      return
    fi
    if [[ "${#TEST_PIDS[@]}" -lt "${MAX_CONCURRENCY}" ]]; then
      return
    fi
  fi

  if [[ "${#TEST_PIDS[@]}" == 0 ]]; then
    return
  fi

  echo "Launched ${#TEST_PIDS[@]} tests."
  echo

  echo "Waiting on " "${TEST_PIDS[@]}"
  wait "${TEST_PIDS[@]}"

  # Emit successful tests first
  for test_dir in "${TEST_DIRS[@]}"; do
    exit_code="$(<"${test_dir}"/exit.txt)"
    if [[ "${exit_code}" -ne 0 ]]; then
      continue
    fi

    cat "${test_dir}/output.txt"
    echo

    grep "test completed in" "${test_dir}/output.txt" >> "${TEST_TIMES_FILE}"
  done

  # Emit failed tests last
  local -i failures=0
  for test_dir in "${TEST_DIRS[@]}"; do
    exit_code="$(<"${test_dir}"/exit.txt)"
    if [[ "${exit_code}" -eq 0 ]]; then
      continue
    fi
    failures+=1

    cat "${test_dir}/output.txt"
    echo

    echo "FAILED: Test failed with exit code ${exit_code}"
    echo

    grep "test completed in" "${test_dir}/output.txt" >> "${TEST_TIMES_FILE}"
  done

  # Emit the time it took to run each test, sorted numerically so the slowest
  # tests are at the end.
  echo "Test timing summary:"
  echo
  cat "${TEST_TIMES_FILE}" \
    | sed -e 's#^\*\*\* \(.*\): test completed in \(.*\) seconds \*\*\*$#\2\t\1#' \
    | sort -n
  echo

  if [[ "${failures}" -gt 0 ]]; then
    echo "*** $(basename "${0}") completed (FAILED): ${failures} tests failed"
    exit 1
  fi

  # Empty the pid and dir arrays
  TEST_PIDS=()
  TEST_DIRS=()
}
readonly -f check_wait_for_integration_tests

# Functions for timing tests...
declare -i TEST_STARTSEC
function start_test() {
  echo "*** Starting test ${1} ***"
  TEST_STARTSEC=$(date +'%s')
}
readonly -f start_test

function end_test() {
  local TEST_ENDSEC=$(date +'%s')
  echo "*** ${1}: test completed in $((TEST_ENDSEC - TEST_STARTSEC)) seconds ***"
}
readonly -f end_test

# Function that checks whether a test can be run for all providers

function get_test_providers() {
  local test_file="$(basename "${1}")"

  # Provider-specific tests are of the form <test>.<provider>.sh
  if [[ ${test_file} == *.*.sh ]]; then
    local providers="$(echo -n "${test_file}" | awk -F . '{ print $(NF-1) }')"

    # Special case the google-v2 tests - run them against google-cls-v2 as well
    if [[ "${providers}" == "google-v2" ]]; then
      echo -n "google-v2 google-cls-v2"
    # Special case the google-batch tests - don't run them when this flag is set
    # To be renabled once batch client library is available in G3
    elif [[ "${providers}" == "google-batch" ]] && [[ "${NO_GOOGLE_BATCH_TESTS:-0}" -eq 1 ]]; then
      echo -n ""
    else
      echo -n "${providers}"
    fi
    return
  fi
  if [[ "${NO_GOOGLE_BATCH_TESTS:-0}" -eq 1 ]]; then
    echo -n "local google-v2 google-cls-v2"
    return
  fi
  case "${test_file}" in
    e2e_command_flag.sh | \
    e2e_dsub_summary.sh | \
    e2e_env_list.py | \
    e2e_image.sh | \
    e2e_input_wildcards.sh | \
    e2e_io.sh | \
    e2e_io_recursive.sh | \
    e2e_logging_content.sh | \
    e2e_logging_fail.sh | \
    e2e_logging_paths.sh | \
    e2e_logging_paths_basic_tasks.sh | \
    e2e_logging_paths_log_suffix_tasks.sh | \
    e2e_logging_paths_pattern_tasks.sh | \
    e2e_non_root.sh | \
    e2e_python.sh | \
    e2e_requester_pays_buckets.sh | \
    e2e_runtime.sh)
      local all_provider_list="${DSUB_PROVIDER:-local google-v2 google-cls-v2 google-batch}"
      ;;
    *)
      local all_provider_list="${DSUB_PROVIDER:-local google-v2 google-cls-v2}"
      ;;
  esac

  echo -n "${all_provider_list}"
}
readonly -f get_test_providers

function string_in_list() {
  local str="${1}"
  local str_list=("${2}")

  local s
  for s in ${str_list[@]}; do
    if [[ "${str}" == "${s}" ]]; then
      return 0
    fi
  done

  return 1
}
readonly -f string_in_list

function provider_in_list() {
  string_in_list "${1}" "${2}"
}
readonly -f provider_in_list

# Begin execution
readonly RUN_TESTS_STARTSEC=$(date +'%s')

# Check usage

if [[ "${1:-}" == "--help" ]]; then
  cat <<EOF
USAGE:
run_tests.sh [unit|e2e|pythonunit]

This script runs all the tests (if unspecified),
or the specified subset of the tests.

The optional argument picks between:
- unit: run test/integration/unit_*
- e2e: run test/integration/e2e_*
- pythonunit: run test/unit/*

For tests in integration/, you can set the DSUB_PROVIDER
environment variable to test only a specific provider;
otherwise all are tested.

You can run these tests individually, like e.g.:

$ export DSUB_PROVIDER=local
$ test/integration/e2e_skip.sh

EOF
  exit 0
fi

readonly SCRIPT_DIR="$(cd "$(dirname "${0}")"; pwd)"

# Normalize the test execution environment to be the dsub root
cd "${SCRIPT_DIR}/.."

declare -a TEST_TYPES=(unit e2e pythonunit)
declare -a TEST_LANGUAGES=(py sh)

# User can specify test types on the command line as arg1
if [[ $# -gt 0 ]]; then
  TEST_TYPES=(${1})
fi

# Build a list of file patterns for the tests, such a "e2e_*" and "unit_*"
declare -a TEST_PATTERNS
if [[ -z "${#TEST_TYPES[@]}" ]]; then
  TEST_PATTERNS=('*')
else
  TEST_PATTERNS=("${TEST_TYPES[@]/%/_*}")
fi

# Append the language extensions to the file patterns to build a list
# of patterns such as "e2e_*.py e2e_*.sh"
declare -a TESTS
for TEST_LANGUAGE in "${TEST_LANGUAGES[@]}"; do
  TESTS+=("${TEST_PATTERNS[@]/%/.${TEST_LANGUAGE}}")
done

echo "Removing ${TEST_OUTPUT_DIR}"
rm -rf "${TEST_OUTPUT_DIR}"

# For each pattern, generate a list of matching tests and run them
declare -a TEST_LIST
for TEST_TYPE in "${TESTS[@]}"; do

  # Run tests in test/unit
  if [[ "${TEST_TYPE}" == "pythonunit_*.py" ]]; then
    if [[ ${NO_PY_MODULE_TESTS:-0} -eq 1 ]]; then
      echo "Test test/unit/*: SKIPPED"
      continue
    fi

    start_test "test/unit"

    # for unit tests, also include the Python unit tests
    if python -m unittest discover -s test/unit/ -p '*_test.py'; then
      echo "Test test/unit/*: PASSED"
    else
      echo "Test test/unit/*: FAILED"
      exit 1
    fi

    end_test "test/unit"

    continue
  fi

  # Run tests in test/integration
  TEST_LIST=($(eval ls "${SCRIPT_DIR}/integration/${TEST_TYPE}" 2>/dev/null || true))

  if [[ -z "${TEST_LIST:-}" ]]; then
    continue
  fi

  for TEST in "${TEST_LIST[@]}"; do
    if [[ ${NO_PY_MODULE_TESTS:-0} -eq 1 ]]; then
      if [[ ${TEST} == *.py ]]; then
        echo "Test ${TEST}: SKIPPED"
        continue
      fi
    fi

    PROVIDER_LIST="$(get_test_providers "${TEST}")"

    # If the user has supplied a DSUB_PROVIDER, then override the PROVIDER_LIST,
    # but only if that provider was in the list.
    if [[ -n "${DSUB_PROVIDER:-}" ]]; then
      if provider_in_list "${DSUB_PROVIDER}" "${PROVIDER_LIST}"; then
        PROVIDER_LIST="${DSUB_PROVIDER}"
      else
        continue
      fi
    fi

    for PROVIDER in ${PROVIDER_LIST}; do
      run_integration_test "${PROVIDER}" "${TEST}"
      check_wait_for_integration_tests
    done
  done

done

check_wait_for_integration_tests "force_wait"

readonly RUN_TESTS_ENDSEC=$(date +'%s')
echo "*** $(basename "${0}") completed (SUCCESS) in $((RUN_TESTS_ENDSEC - RUN_TESTS_STARTSEC)) seconds ***"
