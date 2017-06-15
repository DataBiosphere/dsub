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

Thus using the local runner requires:

* Docker Engine to be installed.
* Sufficient disk space required by submitted tasks.
* Sufficient memory required by submitted tasks.

Note that the local runner supports the `--tasks` parameter. All tasks
submitted will run concurrently.

## Limitations

The local runner does not currently support the `--input-recursive` and
`--output-recursive` flags.

dstat and ddel do not currently work with the local runner.
"""

from collections import namedtuple
from datetime import datetime
import os
import subprocess
import tempfile
import textwrap
from . import base
from ..lib import param_util

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
Task = namedtuple('Task', ['job_id', 'task_id', 'job_status', 'status_message'])


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
    }

  def submit_job(self, job_resources, job_metadata, task_parameters):
    script = job_metadata['script']
    ret = {'job-id': job_metadata['job-id'], 'task-id': []}
    for task_id in range(len(task_parameters)):

      # Set up directories
      task_dir = self._task_directory(job_metadata['job-id'], task_id)
      print 'dsub task directory: ' + task_dir
      print('WARNING: dstat and ddel not yet implemented for the local '
            'provider.')
      task_io = task_parameters[task_id]
      self._mkdir_outputs(task_dir, task_io)

      self._stage_script(task_dir, script.name, script.value)

      # Start the task
      env = self._make_environment(task_io)
      self._run_docker_via_script(task_dir, env, job_resources, job_metadata,
                                  task_io)
      ret['task-id'].append(str(task_id))
    return ret

  def _run_docker_via_script(self, task_dir, env, job_resources, job_metadata,
                             task_io):
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

      stage_data() {{
        true # ensure body is not empty, to avoid error.
        {stage_command}
      }}

      unstage_data() {{
        true # ensure body is not empty, to avoid error.
        {unstage_command}
      }}
      """)
    script_body = textwrap.dedent("""\

      # delete local files
      cleanup() {
        rm -rf "${DATA_DIR}"
      }

      log_info() {
        local prefix=$(date "${DATE_FORMAT}")
        echo "${prefix} I: $@" | tee -a log.txt > status_message.txt
      }

      log_error() {
        local prefix=$(date "${DATE_FORMAT}")
        # Appending so we can see what happened just before the failure.
        echo "${prefix} E: $@" | tee -a log.txt >> status_message.txt
      }

      # correctly log failures and nounset exits
      error() {
        local parent_lineno="$1"
        local code="$2"
        local message="${3:-Error}"
        if [[ $code != "0" ]]; then
          echo "FAILURE" > status.txt
          log_error "${message} on or near line ${parent_lineno}; exiting with status ${code}"
        fi
        cleanup
        # disable further traps
        trap EXIT
        exit "${code}"
      }
      # This will trigger whenever a command returns an error code
      # (exactly like set -e)
      trap 'error ${LINENO} $? Error' ERR
      # This will trigger on all other exits. We disable it before normal
      # exit so we know if it fires it means there's a problem.
      trap 'error ${LINENO} $? "Exit (undefined variable or kill?)"' EXIT

      # Beginning main execution: staging
      cd "${TASK_DIR}"
      echo "RUNNING" > status.txt
      log_info "staging inputs"
      stage_data

      # execution
      log_info "running docker image"
      # we are using :- with ENVIRONMENTS because in the case where there is
      # neither input, output, or explicit environment variable, bash
      # (erronously?) claims that ENVIRONMENTS is unset and fails at this point.
      docker run \
         --name "${NAME}" \
         -w "${DATA_MOUNT_POINT}/${WORKING_DIR}" \
         "${VOLUMES[@]}" \
         --env-file "${ENV_FILE}" \
         "${IMAGE}" \
         bash "${SCRIPT_FILE}" > stdout.txt 2> stderr.txt

      # unstaging
      log_info "copying logs"
      gsutil -q cp stdout.txt ${LOGGING}-stdout.log
      gsutil -q cp stderr.txt ${LOGGING}-stderr.log
      gsutil -q cp log.txt ${LOGGING}.log
      log_info "unstaging outputs"
      unstage_data

      # cleanup
      log_info "cleanup"
      cleanup
      echo "SUCCESS" > status.txt
      log_info "done"
      # disable further traps
      trap EXIT
      """)

    # Build the local runner script
    volumes = ('-v ' + task_dir + '/' + DATA_SUBDIR + '/'
               ':' + DATA_MOUNT_POINT)
    logging = self._make_logging_path(job_resources.logging,
                                      job_metadata['job-id'])
    script = script_header.format(
        volumes=volumes,
        name=job_metadata['job-id'],
        image=job_resources.image,
        script=DATA_MOUNT_POINT + '/' + SCRIPT_DIR + '/' +
        job_metadata['script'].name,
        env_file=task_dir + '/' + 'docker.env',
        data_mount_point=DATA_MOUNT_POINT,
        data_dir=task_dir + '/' + DATA_SUBDIR,
        logging=logging,
        date_format='+%Y/%m/%d %H:%M:%S',
        workingdir=WORKING_DIR,
        stage_command=self._stage_inputs_command(task_dir, task_io),
        unstage_command=self._unstage_outputs_commands(task_dir,
                                                       task_io)) + script_body

    # Write the local runner script
    script_fname = task_dir + '/runner.sh'
    f = open(script_fname, 'wt')
    f.write(script)
    f.close()
    os.chmod(script_fname, 0500)

    # Write the environment variables
    env_vars = env.items() + task_io['envs'] + [
        param_util.EnvParam('DATA_ROOT', DATA_MOUNT_POINT),
        param_util.EnvParam('TMPDIR', DATA_MOUNT_POINT + '/tmp/')
    ]
    env_fname = task_dir + '/docker.env'
    with open(env_fname, 'wt') as f:
      for e in env_vars:
        f.write(e[0] + '=' + e[1] + '\n')

    # Execute the local runner script
    pid = subprocess.Popen([script_fname]).pid
    f = open(task_dir + '/task.pid', 'wt')
    f.write(str(pid) + '\n')
    f.close()
    return pid

  def delete_jobs(self, user_list, job_list, task_list):
    del user_list, job_list, task_list
    raise NotImplementedError()

  def get_task_field(self, task, field, default=None):
    return task._asdict().get(field.replace('-', '_'), default)

  def lookup_job_tasks(self,
                       status_list,
                       user_list=None,
                       job_list=None,
                       task_list=None,
                       max_jobs=0):
    if status_list == ['*']:
      status_list = None
    if user_list == ['*']:
      user_list = None
    if job_list == ['*']:
      job_list = None
    if task_list == ['*']:
      task_list = None

    if user_list:
      raise NotImplementedError(
          'Filtering by user is not yet implemented for the local provider.')
    if not job_list:
      raise NotImplementedError('Local provider currently requires a job list.')
    ret = []
    for j in job_list:
      path = self._provider_root() + '/' + j
      if not os.path.isdir(path):
        continue
      for task_id in os.listdir(path):
        if task_list and task_id not in task_list:
          continue
        ret.append(self._get_task(j, task_id))
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

  # private methods

  def _get_task(self, job_id, task_id):
    """Return a Task object with this task's info."""
    path = self._task_directory(job_id, task_id)
    try:
      status_message = ''
      with open(path + '/status.txt', 'r') as f:
        status = f.readline().strip()
      with open(path + '/status_message.txt', 'r') as f:
        status_message = ''.join(f.readlines())
    except IOError:
      # Files are not there yet.
      # It's a bit of a misnomer, but there is no PENDING status.
      status = 'RUNNING'

    if status == 'RUNNING':
      # double-check running jobs, because it may have been killed but unable to
      # update status (kill -9).
      with open(path + '/task.pid', 'r') as f:
        pid = int(f.readline().strip())
      try:
        os.kill(pid, 0)
      except OSError:
        # process is not running
        status = 'CANCELED'
        status_message = 'Process was killed.'
    return Task(job_id, task_id, status, status_message)

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

  def _task_directory(self, job_id, task_index):
    """The local dir for staging files for that particular task."""
    return self._provider_root() + '/' + job_id + '/' + str(task_index)

  def _make_environment(self, task_io):
    """Return a dictionary of environment variables for the VM."""
    ret = {}
    ins = task_io.get('inputs', [])
    for i in ins:
      ret[i.name] = DATA_MOUNT_POINT + '/' + i.docker_path
    outs = task_io.get('outputs', [])
    for o in outs:
      ret[o.name] = DATA_MOUNT_POINT + '/' + o.docker_path
    return ret

  def _stage_inputs_command(self, task_dir, task_io):
    """Returns a command that will stage inputs."""
    ins = task_io.get('inputs', [])
    commands = []
    for i in ins:
      if i.recursive:
        raise NotImplementedError(
            'input-recursive not yet supported for local execution')
      gcs_file_path = i.value
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + i.docker_path
      commands.append('mkdir -p ' + os.path.dirname(local_file_path))
      commands.append('gsutil -q cp %s %s' % (gcs_file_path, local_file_path))
    return '\n'.join(commands)

  def _mkdir_outputs(self, task_dir, task_io):
    os.makedirs(task_dir + '/' + DATA_SUBDIR + '/' + WORKING_DIR)
    os.makedirs(task_dir + '/' + DATA_SUBDIR + '/tmp')
    outs = task_io.get('outputs', [])
    for o in outs:
      if o.recursive:
        raise NotImplementedError(
            'output-recursive not yet supported for local execution')
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + o.docker_path
      os.makedirs(os.path.dirname(local_file_path))

  def _unstage_outputs_commands(self, task_dir, task_io):
    """copy outputs from local disk to GCS."""
    outs = task_io.get('outputs', [])
    commands = []
    for o in outs:
      local_file_path = task_dir + '/' + DATA_SUBDIR + '/' + o.docker_path
      gcs_file_path = o.remote_uri
      commands.append('gsutil -q cp %s %s' % (local_file_path, gcs_file_path))
    return '\n'.join(commands)

  def _stage_script(self, task_dir, script_name, script_text):
    path = (task_dir + '/' + DATA_SUBDIR + '/' + SCRIPT_DIR + '/' + script_name)
    os.makedirs(os.path.dirname(path))
    f = open(path, 'w')
    f.write(script_text)
    f.write('\n')
    f.close()
    st = os.stat(path)
    os.chmod(path, st.st_mode | 0111)


if __name__ == '__main__':
  pass
