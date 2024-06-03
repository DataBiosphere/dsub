#!/bin/bash

# Copyright 2023 Verily Life Sciences Inc. All Rights Reserved.
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

# This test is intended to check that providers properly write
# stdout and stderr from user commands.
# User commands expect the stdout log file in particular to properly
# contain what the user command emitted to stdout.
# We don't include a test for the aggregate log file as its content
# is provider-specific.

# The STDOUT_MSG and STDER_MSG variables below use a bit of a trick
# to ensure that the values emitted contain a trailing blank line.
# We add a "." to the end of the string and then use:
#   echo -n "${VAR%.}"
# in order to 1) prevent echo from adding a newline and 2) strip the . from
# the end of the string. Otherwise generic variable expansion appears to drop
# trailing newlines from output.
readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

echo "Launching pipeline..."

readonly STDOUT_MSG="$(cat << EOF

This is line 1 to STDOUT.
This is line 2 to STDOUT.

This is line 4 to STDOUT.

.
EOF
)"

readonly STDERR_MSG="$(cat << EOF

This is line 1 to STDERR.
This is line 2 to STDERR.

This is line 4 to STDERR.

.
EOF
)"

run_dsub \
  --command '\
    echo -n '"'${STDOUT_MSG%.}'"' && \
    1>&2 echo -n '"'${STDERR_MSG%.}'"'
  ' \
  --wait

echo
echo "Checking output..."

# Check the results
readonly STDOUT_RESULT_EXPECTED="$(echo -n "${STDOUT_MSG%.}")"

readonly STDOUT_RESULT="$(gsutil cat "${STDOUT_LOG}")"
if ! diff <(echo "${STDOUT_RESULT_EXPECTED}") <(echo "${STDOUT_RESULT}"); then
  echo "STDOUT file does not match expected"
  exit 1
fi

readonly STDERR_RESULT_EXPECTED="$(echo -n "${STDERR_MSG%.}")"

readonly STDERR_RESULT="$(gsutil cat "${STDERR_LOG}")"
if ! diff <(echo "${STDERR_RESULT_EXPECTED}") <(echo "${STDERR_RESULT}"); then
  echo "STDERR file does not match expected"
  exit 1
fi

echo
echo "Output files matches expected:"
echo "******************************"
echo "${STDOUT_RESULT}"
echo "******************************"
echo "${STDERR_RESULT}"
echo "******************************"

echo "SUCCESS"

