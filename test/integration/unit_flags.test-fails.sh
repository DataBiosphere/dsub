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

# unit_flags.sh
#
# Collection of unit tests for dsub command-line flags

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_unit.sh"


# Run the tests
trap "exit_handler" EXIT

mkdir -p "${TEST_TMP}"

# Labels cannot start with a capital letter
run_dsub \
  --label CAPS=bad \
  --command 'echo "Hi"' \
  1> "${TEST_STDOUT}" \
  2> "${TEST_STDERR}" || /bin/true

assert_err_contains \
    'ValueError: Invalid name for label: "CAPS"'

# Correct label, to make sure that goes through
run_dsub \
  --label lowercase_understores=ok-dashes_too \
  --command 'echo "Hi"' \
  1> "${TEST_STDOUT}" \
  2> "${TEST_STDERR}" || /bin/true

# Fancier testing of the label syntax is done in test_param_util.py

assert_err_contains \
    'FailsException'

