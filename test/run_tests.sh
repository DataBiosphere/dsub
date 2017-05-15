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

# For each pattern, generate a list of matching tests and run them
declare -a TEST_LIST
for TEST_TYPE in "${TESTS[@]}"; do

  # Run tests in test/unit
  if [[ "${TEST_TYPE}" == "pythonunit_*.py" ]]; then
    # for unit tests, also include the Python unit tests
    if python -m unittest discover -s test/unit/; then
      echo "Test test/unit/*: PASSED"
    else
      echo "Test test/unit/*: FAILED"
      exit 1
    fi
    continue
  fi

  # Run tests in test/integration
  TEST_LIST=($(eval ls "${SCRIPT_DIR}/integration/${TEST_TYPE}" 2>/dev/null || true))

  if [[ -z "${TEST_LIST:-}" ]]; then
    continue
  fi

  for TEST in "${TEST_LIST[@]}"; do
    if [[ "${TEST}" == *.py ]]; then
      # Execute the test as a module, such as "python -m test.e2e_env_list"
      python -m "test.integration.$(basename "${TEST%.py}")"
    else
      "${TEST}"
    fi

    echo
  done
done

