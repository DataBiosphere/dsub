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
"""Local Docker provider.

# Local provider

The intent of the local backend provider is to enable rapid testing of user task
scripts and easy extension to running at scale on a compute cluster or a cloud
environment. The local provider should simulate the runtime environment such
that going from local development to scaled-up execution only involves changing
`dsub` command-line parameters.

The local provider is not intended for submitting large numbers of concurrent
tasks to a local queue for execution.

## Execution environment

The local provider runs `dsub` tasks locally, in a Docker container.

Input files are staged on your local machine at
${TMPDIR}/dsub-local/job-id/task-id/input_mnt/.

Output files are copied on your local machine at
${TMPDIR}/dsub-local/job-id/task-id/output_mnt/.

Task status files are staged to ${TMPDIR}/tmp/dsub-local/job-id/task-id/.
The task status files include logs and scripts to drive the task.

task-id is the task index, or "task" for a job that didn't specify a list
of tasks.

Thus using the local runner requires:

* Docker Engine to be installed.
* Sufficient disk space required by submitted tasks.
* Sufficient memory required by submitted tasks.

Note that the local runner supports the `--tasks` parameter. All tasks
submitted will run concurrently.
"""

from collections import namedtuple
from datetime import datetime
import os
import signal
import subprocess
import tempfile
import textwrap
import time
from . import base
from ..lib import dsub_util
from ..lib import param_util
from ..lib import providers_util
import yaml

# The local runner allocates space on the host under
#   ${TMPDIR}/dsub-local/
#
# For each task, the on-host directory is
#  ${TMPDIR}/dsub-local/<job-id>/<task-id>
#
# Within the task directory, we create:
#    data: Mount point for user's data
#    docker.env: File of environment variables passed to the Docker container
#    runner.sh: Local runner script
#    status.txt: File for the runner script to record task status (RUNNING,
#    FAILURE, etc.)
#    status_message.txt: File for the runner script to write log messages
#    task.pid: Process ID file for Docker container
#
# From task directory, the data directory is made available to the Docker
# container as /mnt/data. Inside the data directory, the local provider sets up:
#
#   input: files localized from object storage
#   output: files to de-localize to object storage
#
#   script: any code that dsub writes (like the user script)
#   tmp: set TMPDIR in the environment to point here
#
#   workingdir: A workspace directory for user code.
#               This is also the explicit working directory set before the
#               user script runs.

DATA_SUBDIR = 'data'

SCRIPT_DIR = 'script'
WORKING_DIR = 'workingdir'

DATA_MOUNT_POINT = '/mnt/data'

# The task object for this provider. "job_status" is really task status.
Task = namedtuple('Task', [
    'job_id', 'task_id', 'job_status', 'status_message', 'job_name',
    'create_time', 'last_update', 'inputs', 'outputs', 'envs', 'user_id', 'pid'
])


class LocalJobProvider(base.JobProvider):
  """Docker jobs running locally (i.e. on the caller's computer)."""

  def __init__(self):
    self._operations = []
    self.provider_root_cache = None

  def prepare_job_metadata(self, script, job_name, user_id):
    job_name_value = job_name or os.path.basename(script)
    return {
        'job-id': self._make_job_id(job_name_value, user_id),
        'job-name': job_name_value,
        'user-id': user_id,
    }

  def submit_job(self, job_resources, job_metadata, all_task_data):
    script = job_metadata['script']

    launched_tasks = []
    for task_data in all_task_data:
      task_id = task_data.get('task_id')

      # Set up directories
      task_dir = self._task_directory(job_metadata['job-id'], task_id)
      print 'dsub task directory: ' + task_dir
      self._mkdir_outputs(task_dir, task_data)

      self._stage_script(task_dir, script.name, script.value)

      # Start the task
      env = self._make_environment(task_data)
      self._run_docker_via_script(task_dir, env, job_resources, job_metadata,
                                  task_data, task_id)
      self._write_task_metadata(job_metadata, task_data, task_id)
      if task_id is not None:
        launched_tasks.append(str(task_id))

    return {
        'job-id': job_metadata['job-id'],
        'user-id': job_metadata['user-id'],
        'task-id': launched_tasks
    }

  def _run_docker_via_script(self, task_dir, env, job_resources, job_metadata,
                             task_data, task_id):
    script_header = textwrap.dedent("""\
      #!/bin/bash

      # dsub-generated script to start the local Docker container
      # and keep a running status.

      set -o nounset

      readonly VOLUMES=({volumes})
      readonly NAME='{name}'
      readonly IMAGE='{image}'
      # Absolute path to the user's script file inside Docker.
      readonly SCRIPT_FILE='{script}'
      # Mount point for the volume on Docker.
      readonly DATA_MOUNT_POINT='{data_mount_point}'
      # Absolute path to the data.
      readonly DATA_DIR='{data_dir}'
      # Absolute path the the CWD inside Docker.
      readonly WORKING_DIR='{workingdir}'
      # Absolute path to the env config file
      readonly ENV_FILE='{env_file}'
      # Cloud path prefix for log files.
      readonly LOGGING='{logging}'
      readonly DATE_FORMAT='{date_format}'
      # Absolute path to this script's directory.
      readonly TASK_DIR="$(dirname $0)"
      # User to run as (by default)
      readonly MY_UID='{uid}'
      # Set environment variables for recursive input directories
      {export_input_dirs}
      # Set environment variables for recursive output directories
      {export_output_dirs}

      recursive_localize_data() {{
        true # ensure body is not empty, to avoid error.
        {recursive_localize_command}
      }}

      localize_data() {{
        true # ensure body is not empty, to avoid error.
        {localize_command}
      }}

      recursive_delocalize_data() {{
        true # ensure body is not empty, to avoid error.
        {recursive_delocalize_command}
      }}

      delocalize_data() {{
        true # ensure body is not empty, to avoid error.
        {delocalize_command}
      }}
      """)
    script_body = textwrap.dedent("""\
      # Delete local files
      cleanup() {
        echo "Copying the logs before cleanup"
        delocalize_logs
        # Not putting it in status_message because we don't want that to be the
        # last line in case of error.
        echo "cleaning up ${DATA_DIR}"
        # Clean up files written from inside Docker
        2>&1 docker run \\
         --name "${NAME}-cleanup" \\
         --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \\
         "${VOLUMES[@]}" \\
         --env-file "${ENV_FILE}" \\
         "${IMAGE}" \\
         rm -rf "${DATA_MOUNT_POINT}/*" | tee -a log.txt
        # Clean up files staged from outside Docker
        rm -rf "${DATA_DIR}" || echo "sorry, unable to delete ${DATA_DIR}."
      }

      log_info() {
        local prefix=$(date "${DATE_FORMAT}")
        echo "${prefix} I: $@" | tee -a log.txt
        echo "I: $@" > status_message.txt
      }

      log_error() {
        local prefix=$(date "${DATE_FORMAT}")
        echo "${prefix} E: $@" | tee -a log.txt
        # Appending so we can see what happened just before the failure.
        echo "E: $@" >> status_message.txt
      }

      # Correctly log failures and nounset exits
      error() {
        local parent_lineno="$1"
        local code="$2"
        local message="${3:-Error}"
        if [[ $code != "0" ]]; then
          echo "FAILURE" > status.txt
          log_error "${message} on or near line ${parent_lineno}; exiting with status ${code}"
        fi
        cleanup
        # Disable further traps
        trap EXIT
        exit "${code}"
      }
      # This will trigger whenever a command returns an error code
      # (exactly like set -e)
      trap 'error ${LINENO} $? Error' ERR
      # This will trigger on all other exits. We disable it before normal
      # exit so we know if it fires it means there's a problem.
      trap 'error ${LINENO} $? "Exit (undefined variable or kill?)"' EXIT

      fetch_image() {
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

      fetch_image_if_necessary() {
        local image="$1"

        # Remove everything from the first / on
        local prefix="${image%%/*}"

        # Check that the prefix is gcr.io or <location>.gcr.io
        if [[ "${prefix}" == "gcr.io" ]] ||
           [[ "${prefix}" == *.gcr.io ]]; then
          fetch_image "${image}"
        fi
      }

      get_docker_user() {
        # Get the userid and groupid the Docker image is set to run as.
        docker run \\
          --name "${NAME}-get-docker-userid" \\
          "${IMAGE}" \\
          bash -c 'echo "$(id -u):$(id -g)"' 2>> stderr.txt
      }

      docker_recursive_chown() {
        # Calls, in Docker: chown -R $1 $2
        local usergroup="$1"
        local docker_directory="$2"
        # Not specifying a name because Docker refuses to run if two containers
        # have the same name, and it keeps them around for a little bit
        # after they return.
        docker run \\
          --user 0 \\
          "${VOLUMES[@]}" \\
          "${IMAGE}" \\
          chown -R "${usergroup}" "${docker_directory}" >> stdout.txt 2>> stderr.txt
      }

      delocalize_logs() {
        [[ -f stdout.txt ]] && gsutil -q cp stdout.txt "${LOGGING}-stdout.log"
        [[ -f stderr.txt ]] && gsutil -q cp stderr.txt "${LOGGING}-stderr.log"
        [[ -f log.txt ]] && gsutil -q cp log.txt "${LOGGING}.log"
      }

      exit_if_canceled() {
        if [[ -f die ]]; then
          log_info "Job is canceled, stopping Docker container ${NAME}."
          docker stop "${NAME}"
          echo "CANCELED" > status.txt
          log_info "Copy logs to ${LOGGING}, and cleanup"
          cleanup
          trap EXIT
          echo "Canceled, exiting." > status_message.txt
          exit 1
        fi
      }


      # Beginning main execution

      # Copy inputs
      cd "${TASK_DIR}"
      echo "RUNNING" > status.txt
      log_info "Localizing inputs."
      localize_data

      # Handle gcr.io images
      fetch_image_if_necessary "${IMAGE}"

      log_info "Checking image userid."
      USERGROUP="$(get_docker_user)"
      if [[ "${USERGROUP}" != "0:0" ]]; then
        log_info "Ensuring ${USERGROUP} can access ${DATA_MOUNT_POINT}."
        docker_recursive_chown "${USERGROUP}" "${DATA_MOUNT_POINT}"
      fi

      # Begin execution of user script
      FAILURE_MESSAGE=''
      # Disable ERR trap, we want to copy the logs even if Docker fails.
      trap ERR
      log_info "Running Docker image."
      docker run \\
         --detach \\
         --name "${NAME}" \\
         --workdir "${DATA_MOUNT_POINT}/${WORKING_DIR}" \\
         "${VOLUMES[@]}" \\
         --env-file "${ENV_FILE}" \\
         "${IMAGE}" \\
         "${SCRIPT_FILE}"
      exit_if_canceled
      DOCKER_EXITCODE=$(docker wait "${NAME}")
      log_info "Docker returned with status ${DOCKER_EXITCODE}."
      if [[ "${DOCKER_EXITCODE}" != 0 ]]; then
        FAILURE_MESSAGE="Docker returned with status ${DOCKER_EXITCODE} (check stderr)."
      fi
      docker logs "${NAME}" >> stdout.txt 2>> stderr.txt
      # Re-enable trap
      trap 'error ${LINENO} $? Error' ERR

      # Delocalize data
      if [[ $USERGROUP != "0:0" ]]; then
        HOST_USERGROUP="$(id -u):$(id -g)"
        log_info "ensure host user (${HOST_USERGROUP}) can access Docker-written data"
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
      fi
      log_info "Copying outputs."
      delocalize_data

      # Delocalize logs & cleanup
      #
      # Disable further traps (if cleanup fails we don't want to call it
      # recursively)
      trap EXIT
      log_info "Copy logs to ${LOGGING}, and cleanup."
      cleanup
      if [[ -z "${FAILURE_MESSAGE}" ]]; then
        echo "SUCCESS" > status.txt
        log_info "Done"
      else
        echo "FAILURE" > status.txt
        # we want this to be the last line in the log, for dstat to work right.
        log_error "${FAILURE_MESSAGE}"
        exit 1
      fi
      """)

    # Build the local runner script
    volumes = ('-v ' + task_dir + '/' + DATA_SUBDIR + '/'
               ':' + DATA_MOUNT_POINT)
    logging = self._make_logging_path(job_resources.logging,
                                      job_metadata['job-id'])
    script = script_header.format(
        volumes=volumes,
        name=self._get_docker_name(job_metadata['job-id'], task_id),
        image=job_resources.image,
        script=DATA_MOUNT_POINT + '/' + SCRIPT_DIR + '/' +
        job_metadata['script'].name,
        env_file=task_dir + '/' + 'docker.env',
        uid=os.getuid(),
        data_mount_point=DATA_MOUNT_POINT,
        data_dir=task_dir + '/' + DATA_SUBDIR,
        logging=logging,
        date_format='+%Y/%m/%d %H:%M:%S',
        workingdir=WORKING_DIR,
        export_input_dirs=providers_util.build_recursive_localize_env(
            task_dir, task_data.get('inputs', [])),
        recursive_localize_command=self._localize_inputs_recursive_command(
            task_dir, task_data),
        localize_command=self._localize_inputs_command(task_dir, task_data),
        export_output_dirs=providers_util.build_recursive_gcs_delocalize_env(
            task_dir, task_data.get('outputs', [])),
        recursive_delocalize_command=self._delocalize_outputs_recursive_command(
            task_dir, task_data),
        delocalize_command=self._delocalize_outputs_commands(
            task_dir, task_data)) + script_body

    # Write the local runner script
    script_fname = task_dir + '/runner.sh'
    f = open(script_fname, 'wt')
    f.write(script)
    f.close()
    os.chmod(script_fname, 0500)

    # Write the environment variables
    env_vars = env.items() + task_data['envs'] + [
        param_util.EnvParam('DATA_ROOT', DATA_MOUNT_POINT),
        param_util.EnvParam('TMPDIR', DATA_MOUNT_POINT + '/tmp')
    ]
    env_fname = task_dir + '/docker.env'
    with open(env_fname, 'wt') as f:
      for e in env_vars:
        f.write(e[0] + '=' + e[1] + '\n')

    # Execute the local runner script.
    # Redirecting the output to a file ensures that
    # JOBID=$(dsub ...) doesn't block until docker returns.
    runner_log = open(task_dir + '/runner-log.txt', 'wt')
    runner = subprocess.Popen(
        [script_fname], stderr=runner_log, stdout=runner_log)
    pid = runner.pid
    f = open(task_dir + '/task.pid', 'wt')
    f.write(str(pid) + '\n')
    f.close()
    return pid

  def delete_jobs(self, user_list, job_list, task_list):
    # As per the spec, we ignore anything not running.
    tasks = self.lookup_job_tasks(['RUNNING'], user_list, job_list, task_list)
    canceled = []
    cancel_errors = []
    for task in tasks:
      job_id = self.get_task_field(task, 'job-id')
      task_id = self.get_task_field(task, 'task-id')
      task_name = '%s/%s' % (job_id, task_id) if task_id is not None else job_id

      # Try to cancel it for real.
      # First, tell it not to start Docker if it hasn't yet.
      task_dir = self._task_directory(
          self.get_task_field(task, 'job-id'),
          self.get_task_field(task, 'task-id'))
      today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      with open(os.path.join(task_dir, 'die'), 'wt') as f:
        f.write('Operation canceled at %s\n' % today)

      # Next, kill Docker if it's running.
      docker_name = self._get_docker_name_for_task(task)
      try:
        subprocess.check_output(['docker', 'kill', docker_name])
      except subprocess.CalledProcessError as cpe:
        cancel_errors += [
            'Unable to cancel %s: docker error %s:\n%s' %
            (task_name, cpe.returncode, cpe.output)
        ]
        continue

      # The script should have quit in response. If it hasn't, kill it.
      pid = self.get_task_field(task, 'pid', 0)
      if pid <= 0:
        cancel_errors += ['Unable to cancel %s: missing pid.' % task_name]
        continue
      try:
        os.kill(pid, signal.SIGTERM)
      except OSError as err:
        cancel_errors += [
            'Error while canceling %s: kill(%s) failed (%s).' % (task_name, pid,
                                                                 str(err))
        ]
      canceled += [task]

      # Mark the job as 'CANCELED' for the benefit of dstat
      with open(os.path.join(task_dir, 'status.txt'), 'wt') as f:
        f.write('CANCELED\n')
      today = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      msg = 'Operation canceled at %s\n' % today
      with open(os.path.join(task_dir, 'status_message.txt'), 'wt') as f:
        f.write(msg)
      with open(os.path.join(task_dir, 'log.txt'), 'a') as f:
        f.write(msg)

    return (canceled, cancel_errors)

  def get_task_field(self, task, field, default=None):
    # Convert the incoming Task object to a dict.
    # With the exception of "status', the dsub "field" names map directly to the
    # Task members where "-" in the field name is "_" in the Task member name.
    tad = {
        key.replace('_', '-'): value
        for key, value in task._asdict().iteritems()
    }

    # dstat expects to find a field called "status" that contains
    # status if SUCCESS, status-message otherwise
    if field == 'status':
      if tad.get('job-status', '') == 'SUCCESS':
        return 'Success'
      else:
        return tad.get('status-message', None)

    return tad.get(field, default)

  def lookup_job_tasks(self,
                       status_list,
                       user_list=None,
                       job_list=None,
                       job_name_list=None,
                       task_list=None,
                       max_jobs=0):

    status_list = None if status_list == ['*'] else status_list
    user_list = None if user_list == ['*'] else user_list
    job_list = None if job_list == ['*'] else job_list
    job_name_list = None if job_name_list == ['*'] else job_name_list
    task_list = None if task_list == ['*'] else task_list

    if job_name_list:
      raise NotImplementedError(
          'Filtering by job name is not implemented for the local provider'
          ' (%s)' % str(job_name_list))

    if user_list:
      user = dsub_util.get_default_user()
      # We only allow no filtering or filtering by current user.
      if any([u for u in user_list if u != user]):
        raise NotImplementedError(
            'Filtering by user is not implemented for the local provider'
            ' (%s)' % str(user_list))

    ret = []
    if not job_list:
      # Default to every job we know about
      job_list = os.listdir(self._provider_root())
    for j in job_list:
      path = self._provider_root() + '/' + j
      if not os.path.isdir(path):
        continue
      for task_id in os.listdir(path):
        if task_id == 'task':
          task_id = None
        if task_list and task_id not in task_list:
          continue
        ret.append(self._get_task_from_task_dir(j, task_id))
        if max_jobs > 0 and len(ret) > max_jobs:
          break
    return ret

  def get_task_status_message(self, task):
    status = self.get_task_field(task, 'status')
    if status == 'FAILURE':
      return self.get_task_field(task, 'status-message')
    return status

  def get_tasks_completion_messages(self, tasks):
    return [self.get_task_field(task, 'status-message') for task in tasks]

  # Private methods

  def _write_task_metadata(self, job_metadata, task_data, task_id):
    """Write a file with the data needed for dstat."""

    # Build up a dict to dump a YAML file with relevant task details:
    #   job-id: <id>
    #   task-id: <id>
    #   job-name: <name>
    #   inputs:
    #     name: value
    #   outputs:
    #     name: value
    #   envs:
    #     name: value
    data = {
        'job-id': job_metadata['job-id'],
        'task-id': task_id,
        'job-name': job_metadata['job-name'],
    }
    for key in ['inputs', 'outputs', 'envs']:
      if task_data.has_key(key):
        data[key] = {}
        for param in task_data[key]:
          data[key][param.name] = param.value

    task_dir = self._task_directory(job_metadata['job-id'], task_id)
    with open(task_dir + '/meta.yaml', 'wt') as f:
      f.write(yaml.dump(data))

  def _get_task_from_task_dir(self, job_id, task_id):
    """Return a Task object with this task's info."""
    path = self._task_directory(job_id, task_id)
    status = 'uninitialized'
    meta = {}
    last_update = 0
    create_time = 0
    user_id = -1
    pid = -1
    try:
      status_message = ''
      with open(path + '/status.txt', 'r') as f:
        status = f.readline().strip()
      with open(path + '/status_message.txt', 'r') as f:
        status_message = ''.join(f.readlines())
      with open(path + '/meta.yaml', 'r') as f:
        meta = yaml.load('\n'.join(f.readlines()))
      last_update = max([
          os.path.getmtime(path + filename)
          for filename in ['/status.txt', '/status_message.txt', '/meta.yaml']
      ])
      create_time = os.path.getmtime(path + '/task.pid')
      user_id = os.stat(path + '/task.pid').st_uid
    except IOError:
      # Files are not there yet.
      # RUNNING is a misnomer, but there is no PENDING status.
      status = 'RUNNING'
      status_message = 'Process not found yet'
      # Perhaps they crashed before being able to write those files?
      # Check the time.
      try:
        create_time = os.path.getmtime(path + '/task.pid')
        if time.time() - create_time > 60:
          # Time out
          status = 'CANCELED'
          status_message = 'Process failed to start.'
      except IOError:
        # pid file not there, let's say it's still pending.
        pass

    if status == 'RUNNING':
      # Double-check running jobs, because it may have been killed but unable to
      # update status (kill -9).
      try:
        with open(path + '/task.pid', 'r') as f:
          pid = int(f.readline().strip())
        try:
          os.kill(pid, 0)
        except OSError:
          # Process is not running
          status = 'CANCELED'
          status_message = 'Process was killed.'
      except IOError:
        # pid file does not exist, may be from an old version of dsub
        status = 'CANCELED'
        status_message = 'task.pid missing'
    return Task(
        job_id,
        task_id,
        status,
        status_message,
        meta.get('job-name', None),
        datetime.fromtimestamp(create_time),
        datetime.fromtimestamp(last_update) if last_update > 0 else None,
        meta.get('inputs', None),
        meta.get('outputs', None),
        meta.get('envs', None),
        # It's called default_user but actually returns the current user
        dsub_util.get_default_user() if user_id >= 0 else None,
        pid)

  def _provider_root(self):
    if not self.provider_root_cache:
      self.provider_root_cache = tempfile.gettempdir() + '/dsub-local'
    return self.provider_root_cache

  def _make_logging_path(self, gcs_folder_or_file, job_id):
    """Return "gcs_folder/job_id" or "gcs_file" (without .log).

    Matches the Pipelines API behavior: if the user specifies a file
    name for the logs we use that. Otherwise we pick a name.
    In this case we choose the job ID because that's convenient
    for the user.

    Args:
      gcs_folder_or_file: eg. 'gs://bucket/path/myfile.log' or 'gs://bucket/'
      job_id: eg. 'script--foobar-12'

    Returns:
      eg. 'gs://bucket/path/myfile' or 'gs://bucket/script-foobar-12'
    """
    ret = gcs_folder_or_file
    if ret.endswith('.log'):
      ret = ret[:-4]
    elif ret.endswith('/'):
      ret += job_id
    else:
      ret += '/' + job_id
    return ret

  def _make_job_id(self, job_name_value, user_id):
    """Return a job-id string."""

    # We want the job-id to be expressive while also
    # having a low-likelihood of collisions.
    #
    # For expressiveness, we:
    # * use the job name (truncated at 10 characters).
    # * insert the user-id
    # * add a datetime value
    # To have a high likelihood of uniqueness, the datetime value is out to
    # hundredths of a second.
    #
    # The full job-id is:
    #   <job-name>--<user-id>--<timestamp>
    return '%s--%s--%s' % (job_name_value[:10], user_id,
                           datetime.now().strftime('%y%m%d-%H%M%S-%f'))

  def _task_directory(self, job_id, task_id):
    """The local dir for staging files for that particular task."""
    dir_name = 'task' if task_id is None else str(task_id)
    return self._provider_root() + '/' + job_id + '/' + dir_name

  def _make_environment(self, task_data):
    """Return a dictionary of environment variables for the VM."""
    ret = {}
    ins = task_data.get('inputs', [])
    for i in ins:
      ret[i.name] = DATA_MOUNT_POINT + '/' + i.docker_path
    outs = task_data.get('outputs', [])
    for o in outs:
      ret[o.name] = DATA_MOUNT_POINT + '/' + o.docker_path
    return ret

  def _localize_inputs_recursive_command(self, task_dir, task_data):
    """Returns a command that will stage recursive inputs."""
    ins = task_data.get('inputs', [])
    return providers_util.build_recursive_gcs_localize_command(
        task_dir + '/' + DATA_SUBDIR, ins)

  def _get_input_target_path(self, local_file_path):
    """Returns a directory or file path to be the target for "gsutil cp".

    If the filename contains a wildcard, then the target path must
    be a directory in order to ensure consistency whether the source pattern
    contains one or multiple files.


    Args:
      local_file_path: A full path terminating in a file or a file wildcard.

    Returns:
      The path to use as the "gsutil cp" target.
    """

    path, filename = os.path.split(local_file_path)
    if '*' in filename:
      return path + '/'
    else:
      return local_file_path

  def _localize_inputs_command(self, task_dir, task_data):
    """Returns a command that will stage inputs."""
    ins = task_data.get('inputs', [])
    commands = []
    for i in ins:
      if i.recursive:
        continue
      gcs_file_path = i.value
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + i.docker_path
      commands.append('mkdir -p "%s"' % os.path.dirname(local_file_path))
      commands.append('gsutil -q cp "%s" "%s"' %
                      (gcs_file_path,
                       self._get_input_target_path(local_file_path)))

    commands.append(
        textwrap.dedent("""\
    if ! recursive_localize_data; then
      log_error "Recursive localization failed."
      exit 1
    fi
    """))
    return '\n'.join(commands)

  def _mkdir_outputs(self, task_dir, task_data):
    os.makedirs(task_dir + '/' + DATA_SUBDIR + '/' + WORKING_DIR)
    os.makedirs(task_dir + '/' + DATA_SUBDIR + '/tmp')
    outs = task_data.get('outputs', [])
    for o in outs:
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + o.docker_path
      # makedirs errors out if the folder already exists, so check.
      if not os.path.isdir(os.path.dirname(local_file_path)):
        os.makedirs(os.path.dirname(local_file_path))

  def _delocalize_outputs_recursive_command(self, task_dir, task_data):
    outs = task_data.get('outputs', [])
    return providers_util.build_recursive_gcs_delocalize_command(
        task_dir + '/' + DATA_SUBDIR, outs)

  def _delocalize_outputs_commands(self, task_dir, task_data):
    """Copy outputs from local disk to GCS."""
    outs = task_data.get('outputs', [])
    commands = []
    for o in outs:
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + o.docker_path
      gcs_file_path = o.remote_uri
      commands.append('gsutil -q cp "%s" "%s"' % (local_file_path,
                                                  gcs_file_path))
    commands.append(
        textwrap.dedent("""\
    if ! recursive_delocalize_data; then
      log_error "Recursive delocalization failed."
      exit 1
    fi"""))
    return '\n'.join(commands)

  def _stage_script(self, task_dir, script_name, script_text):
    path = (task_dir + '/' + DATA_SUBDIR + '/' + SCRIPT_DIR + '/' + script_name)
    os.makedirs(os.path.dirname(path))
    f = open(path, 'w')
    f.write(script_text)
    f.write('\n')
    f.close()
    st = os.stat(path)
    # Ensure the user script is executable.
    os.chmod(path, st.st_mode | 0100)

  def _get_docker_name_for_task(self, task):
    return self._get_docker_name(
        self.get_task_field(task, 'job-id'),
        self.get_task_field(task, 'task-id'))

  def _get_docker_name(self, job_id, task_id):
    # The name of the docker container is formatted as either:
    #  <job-id>.<task-id>
    # for "task" jobs, or just <job-id> for non-task jobs
    # (those have "None" as the task ID).
    if task_id is None:
      return job_id
    else:
      return '%s.%s' % (job_id, task_id)


if __name__ == '__main__':
  pass
