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

# Setup file to be sourced into e2e_logging_paths*tasks.sh tests.

readonly LOGGING_PATHS_TASKS_FILE_TMPL="${TEST_DIR}/logging_paths_tasks.tsv.tmpl"

function logging_paths_tasks_setup::write_tasks_file() {
  cat "${LOGGING_PATHS_TASKS_FILE_TMPL}" \
      | util::expand_tsv_fields \
      > "${TASKS_FILE}"
}
readonly -f logging_paths_tasks_setup::write_tasks_file

function logging_paths_tasks_setup::dstat_get_logging() {
  local job_id="${1}"
  local task_id="${2}"

  local dstat_out=$(\
    run_dstat \
      --jobs "${job_id}" \
      --status "*" \
      --full \
      --format json)

  # Tasks are listed in reverse order, so use -${task_id}.
  python "${SCRIPT_DIR}"/get_data_value.py \
    "json" "${dstat_out}" "[-${task_id}].logging"
}
readonly -f logging_paths_tasks_setup::dstat_get_logging

function logging_paths_tasks_setup::ddel_task() {
  local job_id="${1}"

  run_ddel --jobs "${job_id}"
}
readonly -f logging_paths_tasks_setup::ddel_task
