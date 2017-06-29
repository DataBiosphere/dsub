#!/bin/bash

# Copyright 2017 Google Inc. All Rights Reserved.
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

# Test that dsub-local's data directory is cleaned up after the job
# returns, whether it succeeded or not.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup.
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Stage a test file.
date | gsutil cp - "${INPUTS}/recursive/deep/today.txt"

readonly TGT_1="${OUTPUTS}/testfile_1.txt"
readonly TGT_2="${OUTPUTS}/testfile_2.txt"
readonly TGT_3="${OUTPUTS}/testfile_3.txt"

function check_jobid {
  local job_id="$1"
  local task_dir="${TMPDIR:-/tmp}/dsub-local/${job_id}/task"
  echo "task dir is: '$task_dir'"
  if ! [[ -d "${task_dir}" ]]; then
    echo "Directory missing: ${task_dir}"
    exit 1
  fi

  if [[ -d "${task_dir}/data" ]]; then
    echo "Directory not deleted: ${task_dir}/data"
    exit 1
  fi
}


#
# Check that /mnt/data is cleaned up when there's an error.
#

echo "Running pipeline that'll fail."
JOB_ID=$(run_dsub \
   --image 'ubuntu' \
   --command 'idontknowhowtounix')
run_dstat --jobs "${JOB_ID}" --wait

check_jobid "${JOB_ID}"

#
# Check that /mnt/data is cleaned up when input files are copied in
# and output is written or copied.
#

echo "Running pipeline with input and output."
JOB_ID=$(run_dsub \
  --image 'ubuntu' \
  --command '
    echo "hello world" > ${OUT_1}
    ls -lad "${IN}/deep/" > "${OUT_2}"
    cp "${IN}/deep/today.txt" "${OUT_3}"
    echo "all good."' \
  --input-recursive IN="${INPUTS}/recursive" \
  --output OUT_1="${TGT_1}" \
  --output OUT_2="${TGT_2}" \
  --output OUT_3="${TGT_3}" \
  --wait)

check_jobid "${JOB_ID}"

for out in "${TGT_1}" "${TGT_2}" "${TGT_3}"; do
  if ! gsutil ls "${out}" > /dev/null; then
    echo "Missing output: ${out}"
    exit 1
  fi
done

echo "SUCCESS"
