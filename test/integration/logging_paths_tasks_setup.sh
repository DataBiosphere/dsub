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

# Several of the logging paths tests specifically test the log file name
# generated, which includes the job-id which is based on the job-name,
# thus we cannot use the new --unique-job-id flag when launching these
# test jobs.
#
# This leads to flaky tests as sometimes jobs are launched concurrently
# and generate the same job identifier.
readonly LOGGING_PATHS_UNIQUE_ID="$(uuidgen)"

function logging_paths_tasks_setup::write_tasks_file() {
  cat "${LOGGING_PATHS_TASKS_FILE_TMPL}" \
      | util::expand_tsv_fields \
      > "${TASKS_FILE}"
}
readonly -f logging_paths_tasks_setup::write_tasks_file

function logging_paths_tasks_setup::get_job_name() {
  # Generate a job name from the test replacing "logging_paths" with "lp"
  #
  # dsub turns the job name into a google label and turns the underscores
  # into labels, so let's start with our job names in that form.
  #
  # Truncate the test name at 10 characters, since that is what dsub will do
  # when it generates the job-id and these logging_paths_* tests are
  # specifically checking that the output log file name is generated correctly.
  echo "lp_${TEST_NAME#logging_paths_}" | tr '_' '-' | cut -c1-10
}
readonly -f logging_paths_tasks_setup::get_job_name

function logging_paths_tasks_setup::run_dsub() {
  run_dsub \
    --name "${JOB_NAME}" \
    --tasks "${TASKS_FILE}" \
    --command 'echo "Test"' \
    --label unique_id="${LOGGING_PATHS_UNIQUE_ID}" \
    "${@}"
}
readonly -f logging_paths_tasks_setup::run_dsub

function logging_paths_tasks_setup::dstat_get_logging() {
  local job_id="${1}"
  local task_id="${2}"

  local dstat_out=$(\
    run_dstat \
      --jobs "${job_id}" \
      --label unique_id="${LOGGING_PATHS_UNIQUE_ID}" \
      --status "*" \
      --full \
      --format json)

  # Tasks are listed in reverse order, so use -${task_id}.
  python3 "${SCRIPT_DIR}"/get_data_value.py \
    "json" "${dstat_out}" "[-${task_id}].logging"
}
readonly -f logging_paths_tasks_setup::dstat_get_logging

function logging_paths_tasks_setup::ddel_task() {
  local job_id="${1}"

  run_ddel --jobs "${job_id}"
}
readonly -f logging_paths_tasks_setup::ddel_task
