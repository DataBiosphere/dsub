#!/bin/bash

# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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

# Test persistent disk abilities.
#
# This test is designed to verify that mounting a Google Persistent Disk works.
# Input files have been placed inside persistent disk sourced by the test image
# used here.
#
# The actual operation performed here is to download a BAM and compute
# the md5, writing it to <filename>.bam.md5.
# and compute its md5, writing it to <filename>.bam.md5.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do io setup
source "${SCRIPT_DIR}/io_setup.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then
  io_setup::image_setup
  echo "Launching pipeline..."
  JOB_ID="$(io_setup::run_dsub_with_mount "${TEST_IMAGE_URL} 50")"
fi

# Do validation
io_setup::check_output
io_setup::check_dstat "${JOB_ID}" false "${TEST_IMAGE_URL}"
