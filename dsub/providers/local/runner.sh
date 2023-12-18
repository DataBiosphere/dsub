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

  if [[ "${rm_data_dir}" == "true" ]]; then
    # Clean up the data directory, except the "script" and "mount" subdirs.
    echo "Cleaning up ${DATA_DIR}"

    # Clean up files written from inside Docker, using the same permissions
    # as the writer.
    2>&1 docker run \
      --name "${NAME}-cleanup" \
      --entrypoint /usr/bin/env \
      --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \
      "${VOLUMES[@]}" \
      --env-file "${ENV_FILE}" \
      "${IMAGE}" \
      "bash" "-c" \
      "rm -rf" \
        "${DATA_MOUNT_POINT}/input" \
        "${DATA_MOUNT_POINT}/output" \
        "${DATA_MOUNT_POINT}/tmp" \
        "${DATA_MOUNT_POINT}/workingdir" \
      | tee -a "${TASK_DIR}/log.txt"

   # Clean up files staged from outside Docker
   rm -rf "${DATA_DIR}/input"
   rm -rf "${DATA_DIR}/output"
   rm -rf "${DATA_DIR}/tmp"
   rm -rf "${DATA_DIR}/workingdir"
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

function get_timestamp() {
  # Using Python instead of /usr/bin/date because the MacOS version cannot get
  # microsecond precision in the format.
  "${PYTHON}" \
    -c 'import datetime; print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))'
}
readonly -f get_timestamp

function write_status() {
  local status="${1}"
  echo "${status}" > "${TASK_DIR}/status.txt"
  case "${status}" in
    SUCCESS|FAILURE|CANCELED)
      echo "$(get_timestamp)" > "${TASK_DIR}/end-time.txt"
      ;;
    RUNNING)
      ;;
    *)
      echo 1>&2 "Unexpected status: ${status}"
      exit 1
      ;;
  esac
}
readonly -f write_status

function write_event() {
  local event="${1}"
  echo "${event},$(get_timestamp)" >> "${TASK_DIR}/events.txt"
}
readonly -f write_event


# Correctly log failures and nounset exits
function error() {
  local parent_lineno="$1"
  local code="$2"
  local message="${3:-Error}"

  # Disable further traps
  trap EXIT
  trap ERR

  if [[ $code != "0" ]]; then
    write_event "fail"
    write_status "FAILURE"
    log_error "${message} on or near line ${parent_lineno}; ${0} exiting with status ${code}"
  fi
  cleanup "false"
  exit "${code}"
}
readonly -f error

function configure_docker_if_necessary() {
  local image="$1"

  # Remove everything from the first / on
  local prefix="${image%%/*}"

  # Check that the prefix is gcr.io or <location>.gcr.io
  if [[ "${prefix}" == "gcr.io" ]] ||
     [[ "${prefix}" == *.gcr.io ]] ||
     [[ "${prefix}" == "pkg.dev" ]] ||
     [[ "${prefix}" == *.pkg.dev ]] ; then
    log_info "Ensuring docker auth is configured for ${prefix}"
    gcloud --quiet auth configure-docker "${prefix}"
  fi
}
readonly -f configure_docker_if_necessary

function get_docker_user() {
  # Get the userid and groupid the Docker image is set to run as.
  docker run \
    --name "${NAME}-get-docker-userid" \
    --entrypoint /usr/bin/env \
    "${IMAGE}" \
    'bash' '-c' 'echo "$(id -u):$(id -g)"' 2>> "${TASK_DIR}/stderr.txt"
}
readonly -f get_docker_user

function docker_recursive_chown() {
  # In Docker: chown recursively on $2 except for the read-only mount dir
  local usergroup="$1"
  local docker_directory="$2"
  # Not specifying a name because Docker refuses to run if two containers
  # have the same name, and it keeps them around for a little bit
  # after they return.
  docker run \
    --user 0 \
    --entrypoint /usr/bin/env \
    "${VOLUMES[@]}" \
    "${IMAGE}" \
    "bash" \
    "-c" \
    "find ${docker_directory} \
       -path '${docker_directory}/mount*' \
       -prune -o -print0 \
       | xargs -0 chown ${usergroup}" \
    >> "${TASK_DIR}/stdout.txt" 2>> "${TASK_DIR}/stderr.txt"
}
readonly -f docker_recursive_chown

function exit_if_canceled() {
  if [[ -f die ]]; then
    log_info "Job is canceled, stopping Docker container ${NAME}."
    docker stop "${NAME}"
    write_event "canceled"
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
PYTHON="$(which python3 || which python)"
if [[ -z "${PYTHON}" ]]; then
  1>&2 echo "ERROR: Could not find python executable"
  exit 1
fi

# Trap errors and handle them instead of using errexit
set +o errexit
trap 'error ${LINENO} $? Error' ERR

# This will trigger on all other exits. We disable it before normal
# exit so we know if it fires it means there's a problem.
trap 'error ${LINENO} $? "Exit (undefined variable or kill?)"' EXIT

# Make sure that ERR traps are inherited by shell functions
set -o errtrace

write_event "start"

# Handle gcr.io images
write_event "pulling-image"
configure_docker_if_necessary "${IMAGE}"

# Copy inputs
cd "${TASK_DIR}"
write_event "localizing-files"
write_status "RUNNING"
log_info "Localizing inputs."
localize_data

log_info "Checking image userid."
log_info "Task image: ${IMAGE}"
DOCKER_USERGROUP="$(get_docker_user)"
if [[ "${DOCKER_USERGROUP}" != "0:0" ]]; then
  log_info "Ensuring docker user (${DOCKER_USERGROUP} can access ${DATA_MOUNT_POINT}."
  docker_recursive_chown "${DOCKER_USERGROUP}" "${DATA_MOUNT_POINT}"
fi

# Begin execution of user script
FAILURE_MESSAGE=''
# Disable ERR trap, we want to copy the logs even if Docker fails.
trap ERR
write_event "running-docker"
log_info "Running Docker image."
docker run \
   --detach \
   --name "${NAME}" \
   --entrypoint /usr/bin/env \
   --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \
   "${VOLUMES[@]}" \
   --env-file "${ENV_FILE}" \
   "${IMAGE}" \
   "bash" "-c" "${SCRIPT_FILE}"

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
write_event "delocalizing-files"
delocalize_data

# Delocalize logs & cleanup
#
# Disable further traps (if cleanup fails we don't want to call it
# recursively)
trap EXIT
log_info "Delocalize logs and cleanup."
cleanup "true"
if [[ -z "${FAILURE_MESSAGE}" ]]; then
  write_event "ok"
  write_status "SUCCESS"
  log_info "Done"
else
  write_event "fail"
  write_status "FAILURE"
  # we want this to be the last line in the log, for dstat to work right.
  log_error "${FAILURE_MESSAGE}"
  exit 1
fi
