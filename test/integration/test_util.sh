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

  printf "${contents}" | grep -v '^$' | sed -e 's#^ *##' > "${file_name}"
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


