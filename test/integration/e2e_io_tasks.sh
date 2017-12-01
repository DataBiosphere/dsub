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

# Basic end to end test, driven by a --tasks file.
#
# This test is designed to verify that file input and output path
# headers in a tasks file work correctly.
#
# The actual operation performed here is to download a BAM and compute
# the md5, writing it to <filename>.bam.md5.
#
# An input file (the BAM) is localized to a subdirectory of the default
# data directory.
# An output file (the MD5) is de-localized from a different subdirectory
# of the default data directory.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do io_task setup
source "${SCRIPT_DIR}/io_tasks_setup.sh"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  io_tasks_setup::write_tasks_file

  echo "Launching pipelines..."

  JOB_ID="$(
    io_tasks_setup::run_dsub \
      "${SCRIPT_DIR}/script_io_test.sh" \
      "${TASKS_FILE}")"

fi

# Do validation
io_tasks_setup::check_output
io_tasks_setup::check_dstat "${JOB_ID}"

