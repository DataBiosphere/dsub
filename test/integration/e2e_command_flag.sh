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

# Basic test of the "--command" flag.
#
# No input files.
# No output files.
# The stdout file is checked for expected output.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

echo "Launching pipeline..."

BOOT_DISK_SIZE=20 \
run_dsub \
  --image "debian:stable-slim" \
  --min-cores "1" \
  --min-ram "3.75" \
  --env VAR1="VAL1" VAR2="VAL2" VAR3="VAL3" \
  --env VAR4="VAL4" \
  --env VAR5="VAL5" \
  --command '\
    # This comment is here to verify that dsub will properly handle
    # a command that starts with a comment, specifically with regards to
    # deriving a usable job name.
    env | grep ^VAR | sort' \
  --wait

echo
echo "Checking output..."

# Check the results
readonly RESULT_EXPECTED=$(cat <<EOF
VAR1=VAL1
VAR2=VAL2
VAR3=VAL3
VAR4=VAL4
VAR5=VAL5
EOF
)

readonly RESULT="$(gsutil cat "${STDOUT_LOG}")"
if ! diff <(echo "${RESULT_EXPECTED}") <(echo "${RESULT}"); then
  echo "Output file does not match expected"
  exit 1
fi

echo
echo "Output file matches expected:"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"

echo "SUCCESS"

