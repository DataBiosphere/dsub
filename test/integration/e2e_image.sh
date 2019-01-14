#!/bin/bash

# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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

# Basic test of using small, 'basic' images.
#
# No input files.
# No output files.
# The stdout file is checked for expected output.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly IMAGE_ARRAY=("bash:4.4" "python:2-slim" "python:3-slim")

# We'd also like to ensure "ubuntu:latest" and "debian:latest" images, but
# other tests cover this.
# ubuntu:
#   e2e_env_tasks.sh
#   e2e_python_api.py
#   e2e_cleanup_local.sh
# debian:
#   e2e_command_flag.sh
#   e2e_error.sh
#   e2e_runtime.sh

readonly RESULT_EXPECTED="Success!"

for image in ${IMAGE_ARRAY[@]}; do
  if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

    echo "Launching pipeline..."

    BOOT_DISK_SIZE=20 \
    run_dsub \
      --image "${image}" \
      --command '\
        echo "Success!"' \
      --wait

  fi

  echo
  echo "Checking output..."

  # Check the results
  RESULT="$(gsutil cat "${STDOUT_LOG}")"
  if ! diff <(echo "${RESULT_EXPECTED}") <(echo "${RESULT}"); then
    echo "Output file does not match expected"
    exit 1
  fi

  echo
  echo "Output file matches expected:"
  echo "*****************************"
  echo "${RESULT}"
  echo "*****************************"
done
echo "SUCCESS"

