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

# script_io_auto.sh
#
# Basic script which will execute:
#   "env": such that callers can verify the environment variables
#   "find": such that callers can verify the contents of /mnt/data

set -o errexit
set -o nounset

echo "BEGIN: env"
env
echo "END: env"

echo "BEGIN: find"
# Emit the output directories (so we can verify they were created)
for VAR in ${!OUTPUT_*}; do
  find "$(dirname "${!VAR}")" -type d
done

# Emit the input files (so we can verify they were localized)
for VAR in ${!INPUT_*}; do
  find "$(dirname "${!VAR}")" -type f
done
echo "END: find"

# Without files to de-localize, a pipeline that has output parameters
# will fail.
# Figure out what to write by iterating over all of the OUTPUT_* variables
# determining whether they are paths to specific filenames or wildcards.
for FILEVAR in ${!OUTPUT_*}; do
  # FILEVAR holds the name of the variable "OUTPUT_0", "OUTPUT_1", etc.
  FILE="${!FILEVAR}"

  DIRNAME=$(dirname "${FILE}")
  FILENAME=$(basename "${FILE}")

  if [[ "${FILENAME}" == "*" ]]; then
    # Just need to write any file
    echo "This is dummy.txt" > "${DIRNAME}"/dummy.txt
  else
    # Need to write the specific output file
    echo "This is ${FILENAME}" > "${FILE}"
  fi
done
