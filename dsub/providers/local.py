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
import datetime
import os
import signal
import string
import subprocess
import tempfile
import textwrap
from . import base
from .._dsub_version import DSUB_VERSION
from dateutil.tz import tzlocal
from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util
import pytz

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
#    log.txt: File for the runner script to write log messages
#    task.pid: Process ID file for task runner
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

_PROVIDER_NAME = 'local'

# Relative path to the runner.sh file within dsub
_RUNNER_SH_RESOURCE = 'dsub/providers/local/runner.sh'

_DATA_SUBDIR = 'data'

_SCRIPT_DIR = 'script'
_WORKING_DIR = 'workingdir'

_DATA_MOUNT_POINT = '/mnt/data'

# Set file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS, job_model.P_LOCAL])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS


def _format_task_name(job_id, task_id):
  """Create a task name from a job-id and a task-id.

  Task names are used internally by dsub as well as by the docker task runner.
  The name is formatted as either "<job-id>.<task-id>" or jobs with multple
  tasks, or just "<job-id>" for jobs with a single task. Task names follow
  formatting conventions allowing them to be safely used as a docker name.

  Args:
    job_id: (str) the job ID.
    task_id: (str) the task ID.

  Returns:
    a task name string.
  """
  if task_id is None:
    docker_name = job_id
  else:
    docker_name = '%s.%s' % (job_id, task_id)

  # Docker container names must match: [a-zA-Z0-9][a-zA-Z0-9_.-]
  # So 1) prefix it with "dsub-" and 2) change all invalid characters to "-".
  return 'dsub-{}'.format(_convert_suffix_to_docker_chars(docker_name))


def _convert_suffix_to_docker_chars(suffix):
  """Rewrite string so that all characters are valid in a docker name suffix."""
  # Docker container names must match: [a-zA-Z0-9][a-zA-Z0-9_.-]
  accepted_characters = string.ascii_letters + string.digits + '_.-'

  def label_char_transform(char):
    if char in accepted_characters:
      return char
    return '-'

  return ''.join(label_char_transform(c) for c in suffix)


class LocalJobProvider(base.JobProvider):
  """Docker jobs running locally (i.e. on the caller's computer)."""

  def __init__(self, resources):
    """Run jobs on your local machine.

    Args:
      resources: module providing access to files packaged with dsub
                 (See dsub/libs/resources.py)
    """
    self._operations = []
    self._resources = resources

  def prepare_job_metadata(self, script, job_name, user_id, create_time):
    job_name_value = job_name or os.path.basename(script)
    if user_id != dsub_util.get_os_user():
      raise ValueError('If specified, the local provider\'s "--user" flag must '
                       'match the current logged-in user.')
    return {
        'job-id': self._make_job_id(job_name_value, user_id, create_time),
        'job-name': job_name_value,
        'user-id': user_id,
        'dsub-version': DSUB_VERSION,
    }

  def submit_job(self, job_descriptor, skip_if_output_present):
    # Validate inputs.
    param_util.validate_submit_args_or_fail(
        job_descriptor,
        provider_name=_PROVIDER_NAME,
        input_providers=_SUPPORTED_FILE_PROVIDERS,
        output_providers=_SUPPORTED_FILE_PROVIDERS,
        logging_providers=_SUPPORTED_LOGGING_PROVIDERS)

    # Launch tasks!
    launched_tasks = []
    for task_view in job_model.task_view_generator(job_descriptor):
      job_metadata = job_descriptor.job_metadata
      job_params = job_descriptor.job_params
      job_resources = job_descriptor.job_resources

      task_descriptor = task_view.task_descriptors[0]

      task_metadata = task_descriptor.task_metadata
      task_params = task_descriptor.task_params
      task_resources = task_descriptor.task_resources

      task_metadata['create-time'] = dsub_util.replace_timezone(
          datetime.datetime.now(), tzlocal())

      inputs = job_params['inputs'] | task_params['inputs']
      outputs = job_params['outputs'] | task_params['outputs']

      if skip_if_output_present:
        # check whether the output's already there
        if dsub_util.outputs_are_present(outputs):
          print 'Skipping task because its outputs are present.'
          continue

      # Set up directories
      task_dir = self._task_directory(
          job_metadata.get('job-id'), task_metadata.get('task-id'))
      self._mkdir_outputs(task_dir,
                          job_params['outputs'] | task_params['outputs'])

      script = job_metadata.get('script')
      self._stage_script(task_dir, script.name, script.value)

      # Start the task
      env = self._make_environment(inputs, outputs)

      self._write_task_metadata(task_dir, task_view)
      self._run_docker_via_script(task_dir, env, job_metadata, job_params,
                                  job_resources, task_metadata, task_params,
                                  task_resources)
      if task_metadata.get('task-id') is not None:
        launched_tasks.append(str(task_metadata.get('task-id')))
      else:
        launched_tasks.append(None)

    if not launched_tasks:
      return {'job-id': dsub_util.NO_JOB}

    return {
        'job-id': job_metadata.get('job-id'),
        'user-id': job_metadata.get('user-id'),
        'task-id': [task_id for task_id in launched_tasks if task_id],
    }

  def _write_source_file(self, dest, body):
    with open(dest, 'wt') as f:
      f.write(body)
    os.chmod(dest, 0500)

  def _run_docker_via_script(self, task_dir, env, job_metadata, job_params,
                             job_resources, task_metadata, task_params,
                             task_resources):
    script_header = textwrap.dedent("""\
      # dsub-generated script containing data for task execution

      readonly VOLUMES=({volumes})
      readonly NAME='{name}'
      readonly IMAGE='{image}'

      # Absolute path to the user's script file inside Docker.
      readonly SCRIPT_FILE='{script}'
      # Mount point for the volume on Docker.
      readonly DATA_MOUNT_POINT='{data_mount_point}'
      # Absolute path to the data.
      readonly DATA_DIR='{data_dir}'
      # Absolute path to the CWD inside Docker.
      readonly WORKING_DIR='{workingdir}'
      # Absolute path to the env config file
      readonly ENV_FILE='{env_file}'
      # Date format used in the logging message prefix.
      readonly DATE_FORMAT='{date_format}'
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
        {localize_command}
        recursive_localize_data
      }}

      recursive_delocalize_data() {{
        true # ensure body is not empty, to avoid error.
        {recursive_delocalize_command}
      }}

      delocalize_data() {{
        {delocalize_command}
        recursive_delocalize_data
      }}

      delocalize_logs() {{
        {delocalize_logs_command}

        delocalize_logs_function "${{cp_cmd}}" "${{prefix}}"
      }}
      """)

    # Build the local runner script
    volumes = ('-v ' + task_dir + '/' + _DATA_SUBDIR + '/'
               ':' + _DATA_MOUNT_POINT)

    script_data = script_header.format(
        volumes=volumes,
        name=_format_task_name(
            job_metadata.get('job-id'), task_metadata.get('task-id')),
        image=job_resources.image,
        script=_DATA_MOUNT_POINT + '/' + _SCRIPT_DIR + '/' +
        job_metadata['script'].name,
        env_file=task_dir + '/' + 'docker.env',
        uid=os.getuid(),
        data_mount_point=_DATA_MOUNT_POINT,
        data_dir=task_dir + '/' + _DATA_SUBDIR,
        date_format='+%Y-%m-%d %H:%M:%S',
        workingdir=_WORKING_DIR,
        export_input_dirs=providers_util.build_recursive_localize_env(
            task_dir, job_params['inputs'] | task_params['inputs']),
        recursive_localize_command=self._localize_inputs_recursive_command(
            task_dir, job_params['inputs'] | task_params['inputs']),
        localize_command=self._localize_inputs_command(
            task_dir, job_params['inputs'] | task_params['inputs']),
        export_output_dirs=providers_util.build_recursive_gcs_delocalize_env(
            task_dir, job_params['outputs'] | task_params['outputs']),
        recursive_delocalize_command=self._delocalize_outputs_recursive_command(
            task_dir, job_params['outputs'] | task_params['outputs']),
        delocalize_command=self._delocalize_outputs_commands(
            task_dir, job_params['outputs'] | task_params['outputs']),
        delocalize_logs_command=self._delocalize_logging_command(
            task_resources.logging_path),
    )

    # Write the runner script and data file to the task_dir
    script_path = os.path.join(task_dir, 'runner.sh')
    script_data_path = os.path.join(task_dir, 'data.sh')
    self._write_source_file(script_path,
                            self._resources.get_resource(_RUNNER_SH_RESOURCE))
    self._write_source_file(script_data_path, script_data)

    # Write the environment variables
    env_vars = set(env.items()) | job_params['envs'] | task_params['envs'] | {
        job_model.EnvParam('DATA_ROOT', _DATA_MOUNT_POINT),
        job_model.EnvParam('TMPDIR', _DATA_MOUNT_POINT + '/tmp')
    }
    env_fname = task_dir + '/docker.env'
    with open(env_fname, 'wt') as f:
      for e in env_vars:
        f.write(e[0] + '=' + e[1] + '\n')

    # Execute the local runner script.
    # Redirecting the output to a file ensures that
    # JOBID=$(dsub ...) doesn't block until docker returns.
    runner_log = open(task_dir + '/runner-log.txt', 'wt')
    runner = subprocess.Popen(
        [script_path, script_data_path], stderr=runner_log, stdout=runner_log)
    pid = runner.pid
    f = open(task_dir + '/task.pid', 'wt')
    f.write(str(pid) + '\n')
    f.close()
    return pid

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    # As per the spec, we ignore anything not running.
    tasks = self.lookup_job_tasks(
        statuses={'RUNNING'},
        user_ids=user_ids,
        job_ids=job_ids,
        task_ids=task_ids,
        labels=labels,
        create_time_min=create_time_min,
        create_time_max=create_time_max)

    canceled = []
    cancel_errors = []
    for task in tasks:
      # Try to cancel it for real.
      # First, tell the runner script to skip delocalization
      task_dir = self._task_directory(
          task.get_field('job-id'), task.get_field('task-id'))
      today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
      with open(os.path.join(task_dir, 'die'), 'wt') as f:
        f.write('Operation canceled at %s\n' % today)

      # Next, kill Docker if it's running.
      docker_name = task.get_docker_name_for_task()

      try:
        subprocess.check_output(['docker', 'kill', docker_name])
      except subprocess.CalledProcessError as cpe:
        cancel_errors += [
            'Unable to cancel %s: docker error %s:\n%s' %
            (docker_name, cpe.returncode, cpe.output)
        ]

      # The script should have quit in response. If it hasn't, kill it.
      pid = task.get_field('pid', 0)
      if pid <= 0:
        cancel_errors += ['Unable to cancel %s: missing pid.' % docker_name]
        continue
      try:
        os.kill(pid, signal.SIGTERM)
      except OSError as err:
        cancel_errors += [
            'Error while canceling %s: kill(%s) failed (%s).' % (docker_name,
                                                                 pid, str(err))
        ]
      canceled += [task]

      # Mark the job as 'CANCELED' for the benefit of dstat
      today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
      with open(os.path.join(task_dir, 'status.txt'), 'wt') as f:
        f.write('CANCELED\n')
      with open(os.path.join(task_dir, 'end-time.txt'), 'wt') as f:
        f.write(today)
      msg = 'Operation canceled at %s\n' % today
      with open(os.path.join(task_dir, 'log.txt'), 'a') as f:
        f.write(msg)

    return (canceled, cancel_errors)

  def lookup_job_tasks(
      self,
      statuses,
      user_ids=None,
      job_ids=None,
      job_names=None,
      task_ids=None,
      labels=None,
      create_time_min=None,
      create_time_max=None,
      max_tasks=0,
      # page_size is ignored for the LocalJobProvider
      page_size=0):

    # 'OR' filtering arguments.
    statuses = None if statuses == {'*'} else statuses
    user_ids = None if user_ids == {'*'} else user_ids
    job_ids = None if job_ids == {'*'} else job_ids
    job_names = None if job_names == {'*'} else job_names
    task_ids = None if task_ids == {'*'} else task_ids
    # 'AND' filtering arguments.
    labels = labels if labels else {}

    # The local provider is intended for local, single-user development. There
    # is no shared queue (jobs run immediately) and hence it makes no sense
    # to look up a job run by someone else (whether for dstat or for ddel).
    # If a user is passed in, we will allow it, so long as it is the current
    # user. Otherwise we explicitly error out.
    approved_users = {dsub_util.get_os_user()}
    if user_ids:
      if user_ids != approved_users:
        raise NotImplementedError(
            'Filtering by user is not implemented for the local provider'
            ' (%s)' % str(user_ids))
    else:
      user_ids = approved_users

    ret = []
    if not job_ids:
      # Default to every job we know about.
      job_ids = os.listdir(self._provider_root())
    for j in job_ids:
      for u in user_ids:
        path = self._provider_root() + '/' + j
        if not os.path.isdir(path):
          continue
        for task_id in os.listdir(path):
          if task_id == 'task':
            task_id = None
          if task_ids and task_id not in task_ids:
            continue

          task = self._get_task_from_task_dir(j, u, task_id)
          if not task:
            continue

          status = task.get_field('status')
          if statuses and status not in statuses:
            continue

          job_name = task.get_field('job-name')
          if job_names and job_name not in job_names:
            continue

          # If labels are defined, all labels must match.
          task_labels = task.get_field('labels')
          labels_match = all(
              [k in task_labels and task_labels[k] == v for k, v in labels])
          if labels and not labels_match:
            continue
          # Check that the job is in the requested age range.
          task_create_time = task.get_field('create-time')
          if not self._datetime_in_range(task_create_time, create_time_min,
                                         create_time_max):
            continue

          ret.append(task)

          if 0 < max_tasks < len(ret):
            break

    ret.sort(key=lambda t: t.get_field('create-time'), reverse=True)
    return ret

  def get_tasks_completion_messages(self, tasks):
    return [task.get_field('status-message') for task in tasks]

  # Private methods
  def _datetime_in_range(self, dt, dt_min=None, dt_max=None):
    """Determine if the provided time is within the range, inclusive."""
    # The pipelines API stores operation create-time with second granularity.
    # We mimic this behavior in the local provider by truncating to seconds.
    dt = dt.replace(microsecond=0)
    if dt_min:
      dt_min = dt_min.replace(microsecond=0)
    else:
      dt_min = dsub_util.replace_timezone(datetime.datetime.min, pytz.utc)
    if dt_max:
      dt_max = dt_max.replace(microsecond=0)
    else:
      dt_max = dsub_util.replace_timezone(datetime.datetime.max, pytz.utc)

    return dt_min <= dt <= dt_max

  def _write_task_metadata(self, task_dir, job_descriptor):
    with open(os.path.join(task_dir, 'meta.yaml'), 'wt') as f:
      f.write(job_descriptor.to_yaml())

  def _read_task_metadata(self, task_dir):
    with open(os.path.join(task_dir, 'meta.yaml'), 'rt') as f:
      return job_model.JobDescriptor.from_yaml(f.read())

  def _get_end_time_from_task_dir(self, task_dir):
    try:
      with open(os.path.join(task_dir, 'end-time.txt'), 'r') as f:
        return dsub_util.replace_timezone(
            datetime.datetime.strptime(f.readline().strip(),
                                       '%Y-%m-%d %H:%M:%S.%f'), tzlocal())
    except (IOError, OSError):
      return None

  def _get_last_update_time_from_task_dir(self, task_dir):
    last_update = 0
    for filename in ['status.txt', 'log.txt', 'meta.yaml']:
      try:
        mtime = os.path.getmtime(os.path.join(task_dir, filename))
        last_update = max(last_update, mtime)
      except (IOError, OSError):
        pass

    return dsub_util.replace_timezone(
        datetime.datetime.fromtimestamp(last_update),
        tzlocal()) if last_update > 0 else None

  def _get_status_from_task_dir(self, task_dir):
    try:
      with open(os.path.join(task_dir, 'status.txt'), 'r') as f:
        return f.readline().strip()
    except (IOError, OSError):
      return None

  def _get_log_detail_from_task_dir(self, task_dir):
    try:
      with open(os.path.join(task_dir, 'log.txt'), 'r') as f:
        return f.read().splitlines()
    except (IOError, OSError):
      return None

  def _get_task_from_task_dir(self, job_id, user_id, task_id):
    """Return a Task object with this task's info."""

    # We need to be very careful about how we read and interpret the contents
    # of the task directory. The directory could be changing because a new
    # task is being created. The directory could be changing because a task
    # is ending.
    #
    # If the meta.yaml does not exist, the task does not yet exist.
    # If the meta.yaml exists, it means the task is scheduled. It does not mean
    # it is yet running.
    # If the task.pid file exists, it means that the runner.sh was started.

    task_dir = self._task_directory(job_id, task_id)

    job_descriptor = self._read_task_metadata(task_dir)
    if not job_descriptor:
      return None

    # If we read up an old task, the user-id will not be in the job_descriptor.
    if not job_descriptor.job_metadata.get('user-id'):
      job_descriptor.job_metadata['user-id'] = user_id

    # Get the pid of the runner
    pid = -1
    try:
      with open(os.path.join(task_dir, 'task.pid'), 'r') as f:
        pid = int(f.readline().strip())
    except (IOError, OSError):
      pass

    # Read the files written by the runner.sh.
    # For new tasks, these may not have been written yet.
    end_time = self._get_end_time_from_task_dir(task_dir)
    last_update = self._get_last_update_time_from_task_dir(task_dir)
    status = self._get_status_from_task_dir(task_dir)
    log_detail = self._get_log_detail_from_task_dir(task_dir)

    # If the status file is not yet written, then mark the task as pending
    if not status:
      status = 'RUNNING'
      log_detail = ['Pending']

    return LocalTask(
        task_status=status,
        log_detail=log_detail,
        job_descriptor=job_descriptor,
        end_time=end_time,
        last_update=last_update,
        pid=pid)

  def _provider_root(self):
    return tempfile.gettempdir() + '/dsub-local'

  def _delocalize_logging_command(self, logging_path):
    """Returns a command to delocalize logs.

    Args:
      logging_path: location of log files.

    Returns:
      eg. 'gs://bucket/path/myfile' or 'gs://bucket/script-foobar-12'
    """

    # Get the logging prefix (everything up to ".log")
    logging_prefix = os.path.splitext(logging_path.uri)[0]

    # Set the provider-specific mkdir and file copy commands
    if logging_path.file_provider == job_model.P_LOCAL:
      mkdir_cmd = 'mkdir -p "%s"\n' % os.path.dirname(logging_prefix)
      cp_cmd = 'cp'
    elif logging_path.file_provider == job_model.P_GCS:
      mkdir_cmd = ''
      cp_cmd = 'gsutil -q cp'
    else:
      assert False

    # Construct the copy command
    copy_logs_cmd = textwrap.dedent("""\
      local cp_cmd="{cp_cmd}"
      local prefix="{prefix}"
    """).format(
        cp_cmd=cp_cmd, prefix=logging_prefix)

    # Build up the command
    body = textwrap.dedent("""\
      {mkdir_cmd}
      {copy_logs_cmd}
    """).format(
        mkdir_cmd=mkdir_cmd, copy_logs_cmd=copy_logs_cmd)

    return body

  def _make_job_id(self, job_name_value, user_id, create_time):
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
                           create_time.strftime('%y%m%d-%H%M%S-%f'))

  def _task_directory(self, job_id, task_id):
    """The local dir for staging files for that particular task."""
    dir_name = 'task' if task_id is None else str(task_id)
    return self._provider_root() + '/' + job_id + '/' + dir_name

  def _make_environment(self, inputs, outputs):
    """Return a dictionary of environment variables for the VM."""
    ret = {}
    for i in inputs:
      ret[i.name] = _DATA_MOUNT_POINT + '/' + i.docker_path
    for o in outputs:
      ret[o.name] = _DATA_MOUNT_POINT + '/' + o.docker_path
    return ret

  def _localize_inputs_recursive_command(self, task_dir, inputs):
    """Returns a command that will stage recursive inputs."""
    data_dir = os.path.join(task_dir, _DATA_SUBDIR)
    provider_commands = [
        providers_util.build_recursive_localize_command(data_dir, inputs,
                                                        file_provider)
        for file_provider in _SUPPORTED_INPUT_PROVIDERS
    ]
    return '\n'.join(provider_commands)

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

  def _localize_inputs_command(self, task_dir, inputs):
    """Returns a command that will stage inputs."""
    commands = []
    for i in inputs:
      if i.recursive:
        continue

      source_file_path = i.uri
      local_file_path = task_dir + '/' + _DATA_SUBDIR + '/' + i.docker_path
      dest_file_path = self._get_input_target_path(local_file_path)

      commands.append('mkdir -p "%s"' % os.path.dirname(local_file_path))

      if i.file_provider in [job_model.P_LOCAL, job_model.P_GCS]:
        # The semantics that we expect here are implemented consistently in
        # "gsutil cp", and are a bit different than "cp" when it comes to
        # wildcard handling, so use it for both local and GCS:
        #
        # - `cp path/* dest/` will error if "path" has subdirectories.
        # - `cp "path/*" "dest/"` will fail (it expects wildcard expansion
        #   to come from shell).
        commands.append('gsutil -q cp "%s" "%s"' % (source_file_path,
                                                    dest_file_path))

    return '\n'.join(commands)

  def _mkdir_outputs(self, task_dir, outputs):
    os.makedirs(task_dir + '/' + _DATA_SUBDIR + '/' + _WORKING_DIR)
    os.makedirs(task_dir + '/' + _DATA_SUBDIR + '/tmp')
    for o in outputs:
      local_file_path = task_dir + '/' + _DATA_SUBDIR + '/' + o.docker_path
      # makedirs errors out if the folder already exists, so check.
      if not os.path.isdir(os.path.dirname(local_file_path)):
        os.makedirs(os.path.dirname(local_file_path))

  def _delocalize_outputs_recursive_command(self, task_dir, outputs):
    cmd_lines = []
    # Generate commands to create any required local output directories.
    for var in outputs:
      if var.recursive and var.file_provider == job_model.P_LOCAL:
        cmd_lines.append('  mkdir -p "%s"' % var.uri.path)
    # Generate local and GCS delocalize commands.
    cmd_lines.append(
        providers_util.build_recursive_delocalize_command(
            os.path.join(task_dir, _DATA_SUBDIR), outputs, job_model.P_GCS))
    cmd_lines.append(
        providers_util.build_recursive_delocalize_command(
            os.path.join(task_dir, _DATA_SUBDIR), outputs, job_model.P_LOCAL))
    return '\n'.join(cmd_lines)

  def _delocalize_outputs_commands(self, task_dir, outputs):
    """Copy outputs from local disk to GCS."""
    commands = []
    for o in outputs:
      if o.recursive:
        continue

      # The destination path is o.uri.path, which is the target directory
      # (rather than o.uri, which includes the filename or wildcard).
      dest_path = o.uri.path
      local_path = task_dir + '/' + _DATA_SUBDIR + '/' + o.docker_path

      if o.file_provider == job_model.P_LOCAL:
        commands.append('mkdir -p "%s"' % dest_path)

      # Use gsutil even for local files (explained in _localize_inputs_command).
      if o.file_provider in [job_model.P_LOCAL, job_model.P_GCS]:
        commands.append('gsutil -q cp "%s" "%s"' % (local_path, dest_path))

    return '\n'.join(commands)

  def _stage_script(self, task_dir, script_name, script_text):
    path = (
        task_dir + '/' + _DATA_SUBDIR + '/' + _SCRIPT_DIR + '/' + script_name)
    os.makedirs(os.path.dirname(path))
    f = open(path, 'w')
    f.write(script_text)
    f.write('\n')
    f.close()
    st = os.stat(path)
    # Ensure the user script is executable.
    os.chmod(path, st.st_mode | 0100)


# The task object for this provider.
_RawTask = namedtuple('_RawTask', [
    'job_descriptor',
    'task_status',
    'log_detail',
    'end_time',
    'last_update',
    'pid',
])


class LocalTask(base.Task):
  """Basic container for task metadata."""

  def __init__(self, *args, **kwargs):
    self._raw = _RawTask(*args, **kwargs)

  def raw_task_data(self):
    """Return a provider-specific representation of task data.

    Returns:
      string of task data from the provider.
    """
    return self._raw._asdict()

  def _get_job_and_task_param(self, job_params, task_params, field):
    return job_params.get(field, set()) | task_params.get(field, set())

  def get_field(self, field, default=None):

    # Most fields should be satisfied from the job descriptor
    job_metadata = self._raw.job_descriptor.job_metadata
    job_params = self._raw.job_descriptor.job_params
    task_metadata = self._raw.job_descriptor.task_descriptors[0].task_metadata
    task_resources = self._raw.job_descriptor.task_descriptors[0].task_resources
    task_params = self._raw.job_descriptor.task_descriptors[0].task_params

    value = None
    if field in [
        'job-id', 'job-name', 'user-id', 'create-time', 'dsub-version'
    ]:
      value = job_metadata.get(field)
    elif field == 'start-time':
      # There's no delay between creation and start since we launch docker
      # immediately for local runs.
      value = job_metadata.get('create-time')
    elif field in ['task-id']:
      value = task_metadata.get(field)
    elif field == 'logging':
      # The job_resources will contain the "--logging" value.
      # The task_resources will contain the resolved logging path.
      # get_field('logging') should currently return the resolved logging path.
      value = task_resources.logging_path
    elif field in ['labels', 'envs']:
      items = self._get_job_and_task_param(job_params, task_params, field)
      value = {item.name: item.value for item in items}
    elif field == 'inputs':
      value = {}
      for field in ['inputs', 'input-recursives']:
        items = self._get_job_and_task_param(job_params, task_params, field)
        value.update({item.name: item.value for item in items})
    elif field == 'outputs':
      value = {}
      for field in ['outputs', 'output-recursives']:
        items = self._get_job_and_task_param(job_params, task_params, field)
        value.update({item.name: item.value for item in items})
    else:
      # Convert the raw Task object to a dict.
      # With the exception of the "status' fields, the dsub field names map
      # directly to the Task members (where "-" in the field name is "_" in the
      # Task member name).
      tad = {
          key.replace('_', '-'): value
          for key, value in self._raw._asdict().iteritems()
      }

      if field == 'status':
        value = tad.get('task-status')
      elif field == 'status-message':
        if tad.get('task-status') == 'SUCCESS':
          value = 'Success'
        else:
          # Return the last line of output
          value = self._last_lines(tad.get('log-detail'), 1)
      elif field == 'status-detail':
        # Return the last three lines of output
        value = self._last_lines(tad.get('log-detail'), 3)
      else:
        value = tad.get(field)

    return value if value is not None else default

  def get_docker_name_for_task(self):
    return _format_task_name(
        self.get_field('job-id'), self.get_field('task-id'))

  @staticmethod
  def _last_lines(value, count):
    """Return the last line(s) as a single (newline delimited) string."""
    if not value:
      return ''

    return '\n'.join(value[-count:])


if __name__ == '__main__':
  pass
