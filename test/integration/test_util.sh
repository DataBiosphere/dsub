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

# test_util.sh
#
# Provides utility functions for dsub tests.

# util::exit_handler
function util::exit_handler() {
  # First grab the exit code
  local code="${?}"

  local tmp_dir="${1:-}"

  if [[ "${code}" -eq 0 ]]; then
    # Only clean-up the temp dir if exiting with success
    if [[ -n "${tmp_dir}" ]]; then
      rm -rf "${tmp_dir}"
    fi
  fi
}
readonly -f util::exit_handler

# util::join
#
# Bash analog to Python string join() routine.
# First argument is a delimiter.
# Remaining arguments will be joined together, separated by the delimiter.
function util::join() {
  local IFS="${1}"
  shift
  echo "${*}"
}
readonly -f util::join

# util::write_tsv_file
#
function util::write_tsv_file() {
  local file_name="${1}"
  local contents="${2}"

  printf -- "${contents}" | grep -v '^$' | sed -e 's#^ *##' > "${file_name}"
}
readonly -f util::write_tsv_file

# util::expand_tsv_fields
#
# Reads stdin as TSV file and emits to stdout a processed version.
#
# The first row is assumed to be a header and is emitted unchanged.
# Remaining rows are processed using bash "eval echo" to execute shell
# (variable) expansion on each field.
#
# This allows for the input to contain fields like:
#   ${OUTPUTS}/job5
# and the output will be something like:
#   gs://my-bucket/dsub/my-test/output/job5
#
function util::expand_tsv_fields() {
  local -i line_no=0

  local -a fields
  while IFS=$'\t\n' read -r -a fields; do
    line_no=$((line_no+1))

    if [[ "${line_no}" -eq 1 ]]; then
      # Emit the header unchanged
      util::join $'\t' "${fields[@]}"
    else
      local -a curr=()
      for field in "${fields[@]}"; do
        curr+=($(eval echo "${field}"))
      done
      util::join $'\t' "${curr[@]}"
    fi
  done
}
readonly -f util::expand_tsv_fields


# get_job_status
#
# Run dstat and return the "status" field for the specified job.
function util::get_job_status() {
  local job_id="$1"

  local dstat_out

  if ! dstat_out=$(
    run_dstat \
      --jobs "${job_id}" \
      --status "*" \
      --full \
      --format json); then
    return 1
  fi

  python "${SCRIPT_DIR}"/get_data_value.py \
    "json" "${dstat_out}" "[0].status"
}
readonly -f util::get_job_status


# wait_for_canceled_status
#
# Wait a maximum number of seconds for a job to reach canceled status.
# If it is canceled status, then return 0, otherwise if it the maximum wait
# is reached, return 1.
function util::wait_for_canceled_status() {
  local job_id="${1}"

  # After calling ddel, we wait a short bit for canceled status.
  # For most providers, this should be very fast, but the Google Pipelines API
  # "operations.cancel" will return success when the operation is internally
  # marked for deletion and there can be a short delay before it is externally
  # marked as CANCELED.
  local max_wait_sec=10
  if [[ "${DSUB_PROVIDER}" == "google" ]]; then
    max_wait_sec=90
  fi

  local status
  echo "Waiting up to ${max_wait_sec} sec for CANCELED status of ${job_id}"
  for ((sec = 0; sec < max_wait_sec; sec += 5)); do
    if ! status="$(util::get_job_status "${job_id}")"; then
      return 1
    fi

    if [[ "${status}" == "CANCELED" ]]; then
      return 0
    fi

    echo "Status: ${status}. Sleep 5s"
    sleep 5s
  done

  return 1
}
readonly -f util::wait_for_canceled_status

function util::is_valid_dstat_datetime() {
  local datetime="${1}"

  # If this fails to parse, it will exit with a non-zero exit code
  python -c '
import datetime
import sys
datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S.%f")
' "${datetime}"
}
readonly -f util::is_valid_dstat_datetime

function util::dstat_yaml_output_value() {
  local dstat_out="${1}"
  local field="${2}"

  python "${SCRIPT_DIR}"/get_data_value.py \
    "yaml" "${dstat_out}" "${field}"
}
readonly -f util::dstat_yaml_output_value

function util::dstat_yaml_job_has_valid_datetime_field() {
  local dstat_out="${1}"
  local field="${2}"

  local value="$(util::dstat_yaml_output_value "${dstat_out}" "${field}")"
  util::is_valid_dstat_datetime "${value}"
}
readonly -f util::dstat_yaml_job_has_valid_datetime_field

function util::dstat_yaml_job_has_valid_end_time() {
  util::dstat_yaml_job_has_valid_datetime_field "${1}" "[0].end-time"
}
readonly -f util::dstat_yaml_job_has_valid_end_time

function util::dstat_yaml_assert_field_equal() {
  local dstat_out="${1}"
  local field="${2}"
  local expected="${3}"

  actual=$(util::dstat_yaml_output_value "${dstat_output}" "${field}")
  if [[ "${actual}" != "${expected}" ]]; then
    2>&1 echo "Assert: actual value for ${field}, ${actual}, does not match expected: ${expected}"
    2>&1 echo "${dstat_output}"
    exit 1
  fi
}
readonly -f util::dstat_yaml_assert_field_equal
