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

# unit_version.sh
#
# Check that the version flag is parsed with the highest priority.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"

readonly VERSION_NUMBER="$(
  python -c "from dsub._dsub_version import DSUB_VERSION; print(DSUB_VERSION)")"
readonly EXPECTED_STRING="dsub version: ${VERSION_NUMBER}"

# Define tests template.
function test_shell_util_version() {
  local subtest="${FUNCNAME[0]}"

  "$@" \
    --version \
    1> "${TEST_STDOUT}" \
    2> "${TEST_STDERR}"

  # Stderr must be empty and stdout should have the version.
  assert_err_empty
  if [[ "$(cat "${TEST_STDOUT}")" != "${EXPECTED_STRING}" ]] ; then
    1>&2 echo "Assert: version was not printed by '$@ --version'"

    test_failed "${subtest}"
  fi

  test_passed "${subtest}"
}
readonly -f test_shell_util_version

function test_version_with_param_expansion() {
  # --version is always highest priority; ensure other flags are ignored
  test_shell_util_version "$@"
  test_shell_util_version "$@" "--project='test-project'"
  test_shell_util_version "$@" "--help"

  echo "SUCCESS: Version flag works for $@"
}
readonly -f test_version_with_param_expansion


# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

echo
test_version_with_param_expansion dsub
test_version_with_param_expansion dstat
test_version_with_param_expansion ddel

