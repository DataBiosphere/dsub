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

# Test a Docker image that sets the USER to a non-root user.
# This test is otherwise identical to e2e_io.sh.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Do io setup
source "${SCRIPT_DIR}/io_setup.sh"

readonly BUILD_DIR="${TEST_TMP}/${TEST_NAME}"
readonly DOCKERFILE="${BUILD_DIR}/Dockerfile"
readonly IMAGE_PROJECT_ID="$(echo "${PROJECT_ID}" | sed s#:#/#)"
readonly IMAGE=gcr.io/${IMAGE_PROJECT_ID}/dsub_usertest:$(date +%Y-%m-%d)_${RANDOM}

function exit_handler() {
  local code="${?}"

  gcloud --quiet container images delete "${IMAGE}"

  return "${code}"
}
readonly -f exit_handler

function check_jobid {
  local job_id="$1"
  local task_dir="${TMPDIR:-/tmp}/dsub-local/${job_id}/task"

  echo "Checking task directory: ${task_dir}"
  if ! [[ -d "${task_dir}" ]]; then
    echo "Directory missing: ${task_dir}"
    exit 1
  fi

  if [[ -d "${task_dir}/data" ]]; then
    echo "Directory not deleted: ${task_dir}/data"
    exit 1
  fi
}
readonly -f check_jobid

trap "exit_handler" EXIT

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Enabling Google Container Builder"
  gcloud service-management enable cloudbuild.googleapis.com

  echo "Creating image using Google Container Builder"

  mkdir -p "${BUILD_DIR}"
  sed -e 's#^ *##' > "${DOCKERFILE}" <<-EOF
    FROM ubuntu:14.04

    RUN adduser test_user \
      --disabled-password --gecos "First Last,RoomNumber,WorkPhone,HomePhone"

    USER test_user
EOF

  gcloud container builds submit "${BUILD_DIR}" --tag "${IMAGE}"

  echo "Launching pipeline..."

  JOB_ID="$(io_setup::run_dsub)"

  if [[ "${DSUB_PROVIDER}" == "local" ]]; then
    # Cleanup is more challenging when the Docker user isn't root,
    # so let's make sure it worked right.
    check_jobid "${JOB_ID}"
  fi

fi

# Do validation
io_setup::check_output
io_setup::check_dstat "${JOB_ID}"
