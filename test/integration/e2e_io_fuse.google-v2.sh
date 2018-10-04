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

# Test gcsfuse abilities.
#
# This test is designed to verify that named GCS bucket (mount)
# command-line parameters work correctly.
#
# The actual operation performed here is to mount to a bucket containing a BAM
# and compute its md5, writing it to <filename>.bam.md5.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do io setup
source "${SCRIPT_DIR}/io_setup.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  JOB_ID=$(io_setup::run_dsub_fuse)

fi

# Do validation
io_setup::check_output
io_setup::check_dstat "${JOB_ID}" false true
