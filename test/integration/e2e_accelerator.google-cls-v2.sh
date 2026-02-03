#!/bin/bash

# Copyright 2020 Verily Life Sciences Inc. All Rights Reserved.
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

# Basic test of using a small attached GPU.
#
# No input files.
# No output files.
# The stdout file is checked for expected output.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

echo "Launching pipeline..."

# Use the python:slim image just to demonstrate that no special image is needed.
# The necessary GPU libraries and files are mounted from the VM into the container.
run_dsub \
  --image 'python:slim' \
  --accelerator-type 'nvidia-tesla-p4' \
  --accelerator-count 1 \
  --command '\
    export LD_LIBRARY_PATH="/usr/local/nvidia/lib64" && \
    /usr/local/nvidia/bin/nvidia-smi' \
  --wait

echo
echo "Checking output..."

# Check the results
RESULT="$(gcloud storage cat "${STDOUT_LOG}")"
if ! echo "${RESULT}" | grep -qi "GPU Memory"; then
  1>&2 echo "GPU Memory not found in the dsub output!"
  1>&2 echo "${RESULT}"
  exit 1
fi

echo
echo "Output file matches expected:"
echo "*****************************"
echo "${RESULT}"
  echo "*****************************"
echo "SUCCESS"

