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

# dsub runner script to orchestrate local docker execution

set -o nounset
set -o errexit

readonly RUNNER_DATA=${1}
source "${RUNNER_DATA}"

# Absolute path to this script's directory.
readonly TASK_DIR="$(dirname $0)"

function get_datestamp() {
  date "${DATE_FORMAT}"
}
readonly -f get_datestamp

function log_info() {
  echo "$(get_datestamp) I: $@" | tee -a "${TASK_DIR}/log.txt"
}
readonly -f log_info

function log_error() {
  echo "$(get_datestamp) E: $@" | tee -a "${TASK_DIR}/log.txt"
}
readonly -f log_error

# Delete local files
function cleanup() {
  local rm_data_dir="${1:-true}"

  log_info "Copying the logs before cleanup"
  delocalize_logs

  # Clean up files staged from outside Docker
  if [[ "${rm_data_dir}" == "true" ]]; then
    echo "cleaning up ${DATA_DIR}"

    # Clean up files written from inside Docker
    2>&1 docker run \
      --name "${NAME}-cleanup" \
      --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \
      "${VOLUMES[@]}" \
      --env-file "${ENV_FILE}" \
      "${IMAGE}" \
      rm -rf "${DATA_MOUNT_POINT}/*" | tee -a "${TASK_DIR}/log.txt"

    rm -rf "${DATA_DIR}" || echo "sorry, unable to delete ${DATA_DIR}."
  fi
}
readonly -f cleanup

function delocalize_logs_function() {
  local cp_cmd="${1}"
  local prefix="${2}"

  if [[ -f "${TASK_DIR}/stdout.txt" ]]; then
    ${cp_cmd} "${TASK_DIR}/stdout.txt" "${prefix}-stdout.log"
  fi
  if [[ -f "${TASK_DIR}/stderr.txt" ]]; then
    ${cp_cmd} "${TASK_DIR}/stderr.txt" "${prefix}-stderr.log"
  fi
  if [[ -f "${TASK_DIR}/log.txt" ]]; then
    ${cp_cmd} "${TASK_DIR}/log.txt" "${prefix}.log"
  fi
}
readonly -f delocalize_logs_function

function write_status() {
  local status="${1}"
  echo "${status}" > "${TASK_DIR}/status.txt"
  case "${status}" in
    SUCCESS|FAILURE|CANCELED)
      # Record the finish time (with microseconds)
      # Prepend "10#" so numbers like 0999... are not treated as octal
      local nanos=$(echo "10#"$(date "+%N"))
      echo $(date "+%Y-%m-%d %H:%M:%S").$((nanos/1000)) \
        > "${TASK_DIR}/end-time.txt"
      ;;
    RUNNING)
      ;;
    *)
      echo 2>&1 "Unexpected status: ${status}"
      exit 1
      ;;
  esac
}
readonly -f write_status

# Correctly log failures and nounset exits
function error() {
  local parent_lineno="$1"
  local code="$2"
  local message="${3:-Error}"

  # Disable further traps
  trap EXIT
  trap ERR

  if [[ $code != "0" ]]; then
    write_status "FAILURE"
    log_error "${message} on or near line ${parent_lineno}; exiting with status ${code}"
  fi
  cleanup "false"
  exit "${code}"
}
readonly -f error

function fetch_image() {
  local image="$1"

  for ((attempt=0; attempt < 3; attempt++)); do
    log_info "Using gcloud to fetch ${image}."
    if gcloud docker -- pull "${image}"; then
      return
    fi
    log_info "Sleeping 30s before the next attempt."
    sleep 30s
  done

  log_error "FAILED to fetch ${image}"
  exit 1
}
readonly -f fetch_image

function fetch_image_if_necessary() {
  local image="$1"

  # Remove everything from the first / on
  local prefix="${image%%/*}"

  # Check that the prefix is gcr.io or <location>.gcr.io
  if [[ "${prefix}" == "gcr.io" ]] ||
     [[ "${prefix}" == *.gcr.io ]]; then
    fetch_image "${image}"
  fi
}
readonly -f fetch_image_if_necessary

function get_docker_user() {
  # Get the userid and groupid the Docker image is set to run as.
  docker run \
    --name "${NAME}-get-docker-userid" \
    "${IMAGE}" \
    bash -c 'echo "$(id -u):$(id -g)"' 2>> "${TASK_DIR}/stderr.txt"
}
readonly -f get_docker_user

function docker_recursive_chown() {
  # Calls, in Docker: chown -R $1 $2
  local usergroup="$1"
  local docker_directory="$2"
  # Not specifying a name because Docker refuses to run if two containers
  # have the same name, and it keeps them around for a little bit
  # after they return.
  docker run \
    --user 0 \
    "${VOLUMES[@]}" \
    "${IMAGE}" \
    chown -R "${usergroup}" "${docker_directory}" \
    >> "${TASK_DIR}/stdout.txt" 2>> "${TASK_DIR}/stderr.txt"
}
readonly -f docker_recursive_chown

function exit_if_canceled() {
  if [[ -f die ]]; then
    log_info "Job is canceled, stopping Docker container ${NAME}."
    docker stop "${NAME}"
    write_status "CANCELED"
    log_info "Delocalize logs and cleanup"
    cleanup "false"
    trap EXIT
    log_info "Canceled, exiting."
    exit 1
  fi
}
readonly -f exit_if_canceled

# Begin main execution

# Trap errors and handle them instead of using errexit
set +o errexit
trap 'error ${LINENO} $? Error' ERR

# This will trigger on all other exits. We disable it before normal
# exit so we know if it fires it means there's a problem.
trap 'error ${LINENO} $? "Exit (undefined variable or kill?)"' EXIT

# Make sure that ERR traps are inherited by shell functions
set -o errtrace

# Copy inputs
cd "${TASK_DIR}"
write_status "RUNNING"
log_info "Localizing inputs."
localize_data

# Handle gcr.io images
fetch_image_if_necessary "${IMAGE}"

log_info "Checking image userid."
DOCKER_USERGROUP="$(get_docker_user)"
if [[ "${DOCKER_USERGROUP}" != "0:0" ]]; then
  log_info "Ensuring docker user (${DOCKER_USERGROUP} can access ${DATA_MOUNT_POINT}."
  docker_recursive_chown "${DOCKER_USERGROUP}" "${DATA_MOUNT_POINT}"
fi

# Begin execution of user script
FAILURE_MESSAGE=''
# Disable ERR trap, we want to copy the logs even if Docker fails.
trap ERR
log_info "Running Docker image."
docker run \
   --detach \
   --name "${NAME}" \
   --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \
   "${VOLUMES[@]}" \
   --env-file "${ENV_FILE}" \
   "${IMAGE}" \
   "${SCRIPT_FILE}"

# Start a log writer in the background
docker logs --follow "${NAME}" \
  >> "${TASK_DIR}/stdout.txt" 2>> "${TASK_DIR}/stderr.txt" &

# Wait for completion
DOCKER_EXITCODE=$(docker wait "${NAME}")
log_info "Docker exit code ${DOCKER_EXITCODE}."
if [[ "${DOCKER_EXITCODE}" != 0 ]]; then
  FAILURE_MESSAGE="Docker exit code ${DOCKER_EXITCODE} (check stderr)."
fi

# If we were canceled during execution, be sure to process as such
exit_if_canceled

# Re-enable trap
trap 'error ${LINENO} $? Error' ERR

# Prepare data for delocalization.
HOST_USERGROUP="$(id -u):$(id -g)"
log_info "Ensure host user (${HOST_USERGROUP}) owns Docker-written data"
# Disable ERR trap, we want to copy the logs even if Docker fails.
trap ERR
docker_recursive_chown "${HOST_USERGROUP}" "${DATA_MOUNT_POINT}"
DOCKER_EXITCODE_2=$?
# Re-enable trap
trap 'error ${LINENO} $? Error' ERR
if [[ "${DOCKER_EXITCODE_2}" != 0 ]]; then
  # Ensure we report failure at the end of the execution
  FAILURE_MESSAGE="chown failed, Docker returned ${DOCKER_EXITCODE_2}."
  log_error "${FAILURE_MESSAGE}"
fi

log_info "Copying outputs."
delocalize_data

# Delocalize logs & cleanup
#
# Disable further traps (if cleanup fails we don't want to call it
# recursively)
trap EXIT
log_info "Delocalize logs and cleanup."
cleanup "true"
if [[ -z "${FAILURE_MESSAGE}" ]]; then
  write_status "SUCCESS"
  log_info "Done"
else
  write_status "FAILURE"
  # we want this to be the last line in the log, for dstat to work right.
  log_error "${FAILURE_MESSAGE}"
  exit 1
fi
