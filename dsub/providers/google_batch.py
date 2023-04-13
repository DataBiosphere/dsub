# Copyright 2022 Google Inc. All Rights Reserved.
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
"""Provider for running jobs on Google Cloud Platform.

This module implements job creation, listing, and canceling using the
Google Batch v1 APIs.
"""

import ast
import json
import os
import sys
import textwrap
from typing import Dict, List, Set

from . import base
from . import google_base
from . import google_batch_operations
from . import google_utils
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util


# pylint: disable=g-import-not-at-top
try:
  from google.cloud import batch_v1
except ImportError:
  # TODO: Remove conditional import when batch library is available
  from . import batch_dummy as batch_v1
# pylint: enable=g-import-not-at-top
_PROVIDER_NAME = 'google-batch'

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS

# Mount point for the data disk in the user's Docker container
_VOLUME_MOUNT_POINT = '/mnt/disks/data'
_DATA_MOUNT_POINT = '/mnt/data'

# These are documented (providers/README.md) as being read/write to user
# commands.
_SCRIPT_DIR = f'{_DATA_MOUNT_POINT}/script'
_TMP_DIR = f'{_DATA_MOUNT_POINT}/tmp'
_WORKING_DIR = f'{_DATA_MOUNT_POINT}/workingdir'

# These are visible to the user; not yet documented, as we'd *like* to
# find a way to have them visible only to the logging tasks.
_BATCH_LOG_FILE_PATH = f'{_VOLUME_MOUNT_POINT}/.log.txt'
_LOG_FILE_PATH = f'{_DATA_MOUNT_POINT}/.log.txt'
_LOGGING_DIR = f'{_DATA_MOUNT_POINT}/.logging'

_LOG_FILTER_VAR = '_LOG_FILTER_REPR'
_LOG_FILTER_SCRIPT_PATH = f'{_DATA_MOUNT_POINT}/.log_filter_script.py'

# _LOG_FILTER_PYTHON is a block of Python code to execute in both the
# "continuous_logging" and "final_logging" tasks.
#
# Batch API gives us one log file that is an interleave of all of the task
# outputs to both STDOUT and STDERR. A typical line looks something like:
#
# [batch_task_logs]<date> logging.go:227: INFO: [task_id:<task>]
#  Task task/<task>-0/0/0/1, STDOUT: <message emitted from the task>
#
# Parsing text inputs can be fragile, so we try to be loose but accurate with
# our assumptions on log file format.
#
# The way the following Python executes is to
# - Open the log file
#   - For each line
#   - If the line is *not* associated with the "user command", only emit it
#     to the aggregate log (and emit it as-is)
#   - AND if we find "<stuff>/<user_task_number>, STDOUT: " (or "...STDERR: ")
#     - emit the task-generated message to either the stdout or stderr log file
#
# Thus the stdout and stderr log files only contain output from the user task,
# while the aggregate log contains everything.
#
# There's tricky behavior to handle blank lines in the GCP Batch log file.
# What we observe is that *every* valid message emitted to the log is followed
# by a blank line.
# So we'd like to filter out those blank lines, but we need to be careful
# because a user command can emit blank lines, *which we want to preserve.*
# Thus when we see a blank line, we "hold" it as we don't know if we are going
# to print it until we see the next line.
#
# Note that to find the user task messages, we set up search strings
# (STDOUT_STR, STDERR_STR) which look something like: "/3, STDOUT: "

_LOG_FILTER_PYTHON = textwrap.dedent("""
import sys
from pathlib import Path

INFILE_PATH = sys.argv[1]
LOG_FILE_PATH = sys.argv[2]
STDOUT_FILE_PATH = sys.argv[3]
STDERR_FILE_PATH = sys.argv[4]
EMIT_UNEXPECTED_PATH = sys.argv[5]
USER_TASK = sys.argv[6]

EMIT_UNEXPECTED_ONCE = Path(EMIT_UNEXPECTED_PATH)

STDOUT_STR = f"/{USER_TASK}, STDOUT: "
STDERR_STR = f"/{USER_TASK}, STDERR: "

with open(INFILE_PATH) as IN_FILE:

  LOG_FILE = open(LOG_FILE_PATH, "w")
  STDOUT_FILE = open(STDOUT_FILE_PATH, "w")
  STDERR_FILE = open(STDERR_FILE_PATH, "w")

  BLANK_LINE_HOLD = False
  WHICH_STD_FILE = None
  line_no = 0
  for line in IN_FILE:
    line_no += 1

    line = line.splitlines()[0]
    if not line:
      BLANK_LINE_HOLD = True
      continue
    elif line.startswith("[batch_task_logs]"):
      BLANK_LINE_HOLD = False
      WHICH_STD_FILE = None
    else:
      if BLANK_LINE_HOLD and WHICH_STD_FILE:
        print(file=LOG_FILE)
        print(file=WHICH_STD_FILE)

    print(line, file=LOG_FILE)

    stdout = line.find(STDOUT_STR)
    stderr = line.find(STDERR_STR)
    stdout = sys.maxsize if stdout == -1 else stdout
    stderr = sys.maxsize if stderr == -1 else stderr

    if stdout < stderr:
      print(line[stdout + len(STDOUT_STR):], file=STDOUT_FILE)
      WHICH_STD_FILE = STDOUT_FILE
    elif stdout > stderr:
      print(line[stderr + len(STDERR_STR):], file=STDERR_FILE)
      WHICH_STD_FILE = STDERR_FILE
    elif WHICH_STD_FILE:
      print(line, file=WHICH_STD_FILE)
    elif not EMIT_UNEXPECTED_ONCE.is_file():
      print(f"Unexpected log format at line {line_no}: {line}")
      EMIT_UNEXPECTED_ONCE.touch()
""")

_LOG_CP = textwrap.dedent("""
  cp "{log_file_path}" .
  python3 "{log_filter_script_path}" \
      "{log_file_path}" \
      "${{LOGGING_DIR}}/log.txt" \
      "${{LOGGING_DIR}}/stdout.txt" \
      "${{LOGGING_DIR}}/stderr.txt" \
      "${{LOGGING_DIR}}/emit_unexpected_sentinel.txt" \
      "{user_action}"

  gsutil_cp "${{LOGGING_DIR}}/stdout.txt" "${{STDOUT_PATH}}" "text/plain" "${{USER_PROJECT}}" &
  STDOUT_PID=$!
  gsutil_cp "${{LOGGING_DIR}}/stderr.txt" "${{STDERR_PATH}}" "text/plain"  "${{USER_PROJECT}}" &
  STDERR_PID=$!
  gsutil_cp "${{LOGGING_DIR}}/log.txt" "${{LOGGING_PATH}}" "text/plain" "${{USER_PROJECT}}" &
  LOG_PID=$!

  wait "${{STDOUT_PID}}"
  wait "${{STDERR_PID}}"
  wait "${{LOG_PID}}"
""")

_FINAL_LOGGING_CMD = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  readonly LOGGING_DIR="{logging_dir}"

  # Flag the continuous logging command to stop
  touch "${{LOGGING_DIR}}/.stop_logging"

  {log_msg_fn}
  {gsutil_cp_fn}

  {log_cp}
""")

# Keep logging until the final logging action starts
_CONTINUOUS_LOGGING_CMD = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  readonly LOGGING_DIR="{logging_dir}"

  {log_msg_fn}
  {gsutil_cp_fn}

  # Make sure the logging work directory exists
  mkdir -p "${{LOGGING_DIR}}"

  # Prep the log filter script
  echo "${{{log_filter_var}}}" \
    | python -c '{python_decode_script}' \
    > "{log_filter_script_path}"
  chmod a+x "{log_filter_script_path}"

  # Make sure the log file exists
  touch "{log_file_path}"

  while [[ ! -e "${{LOGGING_DIR}}/.stop_logging" ]]; do
    {log_cp}

    sleep "{log_interval}"
  done
""")


class GoogleBatchOperation(base.Task):
  """Task wrapper around a Batch API Job object."""

  def __init__(self, operation_data: batch_v1.types.Job):
    self._op = operation_data
    self._job_descriptor = self._try_op_to_job_descriptor()

  def raw_task_data(self):
    return self._op

  def _try_op_to_job_descriptor(self):
    # The _META_YAML_REPR field in the 'prepare' action enables reconstructing
    # the original job descriptor.
    # TODO: Currently, we set the environment across all runnables
    # We really only want the env for the prepare action (runnable) here.
    env = google_batch_operations.get_environment(self._op)
    if not env:
      return

    meta = env.get(google_utils.META_YAML_VARNAME)
    if not meta:
      return

    return job_model.JobDescriptor.from_yaml(ast.literal_eval(meta))

  def get_field(self, field: str, default: str = None):
    """Returns a value from the operation for a specific set of field names.

    This is the implementation of base.Task's abstract get_field method. See
    base.py get_field for more details.

    Args:
      field: a dsub-specific job metadata key
      default: default value to return if field does not exist or is empty.

    Returns:
      A text string for the field or a list for 'inputs'.

    Raises:
      ValueError: if the field label is not supported by the operation
    """
    value = None
    if field == 'internal-id':
      value = self._op.name
    elif field == 'user-project':
      if self._job_descriptor:
        value = self._job_descriptor.job_metadata.get(field)
    elif field in [
        'job-id',
        'job-name',
        'task-id',
        'task-attempt',
        'user-id',
        'dsub-version',
    ]:
      value = google_batch_operations.get_label(self._op, field)
    elif field == 'task-status':
      value = self._operation_status()
    elif field == 'logging':
      if self._job_descriptor:
        # The job_resources will contain the "--logging" value.
        # The task_resources will contain the resolved logging path.
        # Return the resolved logging path.
        task_resources = self._job_descriptor.task_descriptors[0].task_resources
        value = task_resources.logging_path
    elif field in ['envs', 'labels']:
      if self._job_descriptor:
        items = providers_util.get_job_and_task_param(
            self._job_descriptor.job_params,
            self._job_descriptor.task_descriptors[0].task_params,
            field,
        )
        value = {item.name: item.value for item in items}
    elif field in [
        'inputs',
        'outputs',
        'input-recursives',
        'output-recursives',
    ]:
      if self._job_descriptor:
        value = {}
        items = providers_util.get_job_and_task_param(
            self._job_descriptor.job_params,
            self._job_descriptor.task_descriptors[0].task_params,
            field,
        )
        value.update({item.name: item.value for item in items})
    elif field == 'mounts':
      if self._job_descriptor:
        items = providers_util.get_job_and_task_param(
            self._job_descriptor.job_params,
            self._job_descriptor.task_descriptors[0].task_params,
            field,
        )
        value = {item.name: item.value for item in items}
    elif field == 'provider':
      return _PROVIDER_NAME
    elif field == 'provider-attributes':
      # TODO: This needs to return instance (VM) metadata
      value = {}
    elif field == 'events':
      # TODO: This needs to return a list of events
      value = []
    elif field == 'script-name':
      if self._job_descriptor:
        value = self._job_descriptor.job_metadata.get(field)
    elif field == 'script':
      value = self._try_op_to_script_body()
    elif field == 'create-time' or field == 'start-time':
      # TODO: Does Batch offer a start or end-time?
      # Check http://shortn/_FPYmD1weUF
      ds = google_batch_operations.get_create_time(self._op)
      value = google_base.parse_rfc3339_utc_string(ds)
    elif field == 'end-time' or field == 'last-update':
      # TODO: Does Batch offer an end-time?
      # Check http://shortn/_FPYmD1weUF
      ds = google_batch_operations.get_update_time(self._op)
      if ds:
        value = google_base.parse_rfc3339_utc_string(ds)
    elif field == 'status':
      value = self._operation_status()
    elif field == 'status-message':
      value = self._operation_status_message()
    elif field == 'status-detail':
      value = self._operation_status_message()
    else:
      raise ValueError(f'Unsupported field: "{field}"')

    return value if value else default

  def _try_op_to_script_body(self):
    # TODO: Currently, we set the environment across all runnables
    # We really only want the env for the prepare action (runnable) here.
    env = google_batch_operations.get_environment(self._op)
    if env:
      return ast.literal_eval(env.get(google_utils.SCRIPT_VARNAME))

  def _operation_status(self):
    """Returns the status of this operation.

    Raises:
      ValueError: if the operation status cannot be determined.

    Returns:
      A printable status string (RUNNING, SUCCESS, CANCELED or FAILURE).
    """
    if not google_batch_operations.is_done(self._op):
      return 'RUNNING'
    if google_batch_operations.is_success(self._op):
      return 'SUCCESS'
    if google_batch_operations.is_canceled():
      return 'CANCELED'
    if google_batch_operations.is_failed(self._op):
      return 'FAILURE'

    raise ValueError(
        'Status for operation {} could not be determined'.format(
            self._op['name']
        )
    )

  def _operation_status_message(self):
    # TODO: This is intended to grab as much detail as possible
    # Currently, just grabbing the description field from the last status_event
    status_events = google_batch_operations.get_status_events(self._op)
    if status_events:
      return status_events[-1].description


class GoogleBatchBatchHandler(object):
  """Implement the HttpBatch interface to enable simple serial batches."""

  def __init__(self, callback):
    self._cancel_list = []
    self._response_handler = callback

  def add(self, cancel_fn, request_id):
    self._cancel_list.append((request_id, cancel_fn))

  def execute(self):
    for request_id, cancel_fn in self._cancel_list:
      response = None
      exception = None
      try:
        response = cancel_fn.result()
      except:  # pylint: disable=bare-except
        exception = sys.exc_info()[1]

      self._response_handler(request_id, response, exception)


class GoogleBatchJobProvider(google_utils.GoogleJobProviderBase):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(
      self, dry_run: bool, project: str, location: str, credentials=None
  ):
    self._dry_run = dry_run
    self._location = location
    self._project = project

  def _batch_handler_def(self):
    return GoogleBatchBatchHandler

  def _operations_cancel_api_def(self):
    return batch_v1.BatchServiceClient().delete_job

  def _get_create_time_filters(self, create_time_min, create_time_max):
    # TODO: Currently, Batch API does not support filtering by create t.
    return []

  def _get_logging_env(self, logging_uri, user_project, include_filter_script):
    """Returns the environment for actions that copy logging files."""
    if not logging_uri.endswith('.log'):
      raise ValueError('Logging URI must end in ".log": {}'.format(logging_uri))

    logging_prefix = logging_uri[: -len('.log')]
    env = {
        'LOGGING_PATH': '{}.log'.format(logging_prefix),
        'STDOUT_PATH': '{}-stdout.log'.format(logging_prefix),
        'STDERR_PATH': '{}-stderr.log'.format(logging_prefix),
        'USER_PROJECT': user_project,
    }
    if include_filter_script:
      env[_LOG_FILTER_VAR] = repr(_LOG_FILTER_PYTHON)

    return env

  def _create_batch_request(
      self,
      task_view: job_model.JobDescriptor,
      job_id,
      all_envs: List[batch_v1.types.Environment],
  ):
    job_metadata = task_view.job_metadata
    job_params = task_view.job_params
    job_resources = task_view.job_resources
    task_metadata = task_view.task_descriptors[0].task_metadata
    task_params = task_view.task_descriptors[0].task_params
    task_resources = task_view.task_descriptors[0].task_resources

    # Set up VM-specific variables
    datadisk_volume = google_batch_operations.build_volume(
        disk=google_utils.DATA_DISK_NAME, path=_VOLUME_MOUNT_POINT
    )

    # Set up the task labels
    # pylint: disable=g-complex-comprehension
    labels = {
        label.name: label.value if label.value else ''
        for label in google_base.build_pipeline_labels(
            job_metadata, task_metadata
        )
        | job_params['labels']
        | task_params['labels']
    }
    # pylint: enable=g-complex-comprehension

    # Set local variables for the core pipeline values
    script = task_view.job_metadata['script']

    # Track 0-based runnable indexes for cross-task awareness
    user_action = 3

    continuous_logging_cmd = _CONTINUOUS_LOGGING_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        gsutil_cp_fn=google_utils.GSUTIL_CP_FN,
        log_filter_var=_LOG_FILTER_VAR,
        log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
        python_decode_script=google_utils.PYTHON_DECODE_SCRIPT,
        logging_dir=_LOGGING_DIR,
        log_file_path=_LOG_FILE_PATH,
        log_cp=_LOG_CP.format(
            log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
            log_file_path=_LOG_FILE_PATH,
            user_action=user_action,
        ),
        log_interval=job_resources.log_interval or '60s',
    )

    logging_cmd = _FINAL_LOGGING_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        gsutil_cp_fn=google_utils.GSUTIL_CP_FN,
        log_filter_var=_LOG_FILTER_VAR,
        log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
        python_decode_script=google_utils.PYTHON_DECODE_SCRIPT,
        logging_dir=_LOGGING_DIR,
        log_file_path=_LOG_FILE_PATH,
        log_cp=_LOG_CP.format(
            log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
            log_file_path=_LOG_FILE_PATH,
            user_action=user_action,
        ),
    )

    # Set up command and environments for the prepare, localization, user,
    # and de-localization actions
    script_path = os.path.join(_SCRIPT_DIR, script.name)
    user_project = task_view.job_metadata['user-project'] or ''

    prepare_command = google_utils.PREPARE_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        mk_runtime_dirs=google_utils.make_runtime_dirs_command(
            _SCRIPT_DIR, _TMP_DIR, _WORKING_DIR
        ),
        script_var=google_utils.SCRIPT_VARNAME,
        python_decode_script=google_utils.PYTHON_DECODE_SCRIPT,
        script_path=script_path,
        mk_io_dirs=google_utils.MK_IO_DIRS,
    )
    # pylint: disable=line-too-long

    continuous_logging_env = google_batch_operations.build_environment(
        self._get_logging_env(
            task_resources.logging_path.uri, user_project, True
        )
    )
    final_logging_env = google_batch_operations.build_environment(
        self._get_logging_env(
            task_resources.logging_path.uri, user_project, False
        )
    )

    # Build the list of runnables (aka actions)
    runnables = []

    runnables.append(
        # logging
        google_batch_operations.build_runnable(
            run_in_background=True,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=continuous_logging_env,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=['-c', continuous_logging_cmd],
        )
    )

    runnables.append(
        # prepare
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=None,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=['-c', prepare_command],
        )
    )

    runnables.append(
        # localization
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=None,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=[
                '-c',
                google_utils.LOCALIZATION_CMD.format(
                    log_msg_fn=google_utils.LOG_MSG_FN,
                    recursive_cp_fn=google_utils.GSUTIL_RSYNC_FN,
                    cp_fn=google_utils.GSUTIL_CP_FN,
                    cp_loop=google_utils.LOCALIZATION_LOOP,
                ),
            ],
        )
    )

    runnables.append(
        # user-command
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=job_resources.image,
            environment=None,
            entrypoint='/usr/bin/env',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=[
                'bash',
                '-c',
                google_utils.USER_CMD.format(
                    tmp_dir=_TMP_DIR,
                    working_dir=_WORKING_DIR,
                    user_script=script_path,
                ),
            ],
        )
    )

    runnables.append(
        # delocalization
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=None,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}:ro'],
            commands=[
                '-c',
                google_utils.LOCALIZATION_CMD.format(
                    log_msg_fn=google_utils.LOG_MSG_FN,
                    recursive_cp_fn=google_utils.GSUTIL_RSYNC_FN,
                    cp_fn=google_utils.GSUTIL_CP_FN,
                    cp_loop=google_utils.DELOCALIZATION_LOOP,
                ),
            ],
        )
    )

    runnables.append(
        # final_logging
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=True,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=final_logging_env,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=['-c', logging_cmd],
        ),
    )

    # Prepare the VM (resources) configuration. The InstancePolicy describes an
    # instance type and resources attached to each VM. The AllocationPolicy
    # describes when, where, and how compute resources should be allocated
    # for the Job.
    disk = google_batch_operations.build_persistent_disk(
        size_gb=job_resources.disk_size,
        disk_type=job_resources.disk_type or job_model.DEFAULT_DISK_TYPE,
    )
    attached_disk = google_batch_operations.build_attached_disk(
        disk=disk, device_name=google_utils.DATA_DISK_NAME
    )
    instance_policy = google_batch_operations.build_instance_policy(
        attached_disk
    )
    ipt = google_batch_operations.build_instance_policy_or_template(
        instance_policy
    )
    allocation_policy = google_batch_operations.build_allocation_policy([ipt])
    logs_policy = google_batch_operations.build_logs_policy(
        batch_v1.LogsPolicy.Destination.PATH, _BATCH_LOG_FILE_PATH
    )

    # Bring together the task definition(s) and build the Job request.
    task_spec = google_batch_operations.build_task_spec(
        runnables=runnables, volumes=[datadisk_volume]
    )
    task_group = google_batch_operations.build_task_group(
        task_spec, all_envs, task_count=len(all_envs), task_count_per_node=1
    )

    job = google_batch_operations.build_job(
        [task_group], allocation_policy, labels, logs_policy
    )

    job_request = batch_v1.CreateJobRequest(
        parent=f'projects/{self._project}/locations/{self._location}',
        job=job,
        job_id=job_id,
    )
    # pylint: enable=line-too-long
    return job_request

  def _submit_batch_job(self, request) -> str:
    client = batch_v1.BatchServiceClient()
    job_response = client.create_job(request=request)
    op = GoogleBatchOperation(job_response)
    print(f'Provider internal-id (operation): {job_response.name}')
    return op.get_field('task-id')

  def _create_env_for_task(
      self, task_view: job_model.JobDescriptor
  ) -> Dict[str, str]:
    job_params = task_view.job_params
    task_params = task_view.task_descriptors[0].task_params

    # Set local variables for the core pipeline values
    script = task_view.job_metadata['script']
    user_project = task_view.job_metadata['user-project'] or ''

    envs = job_params['envs'] | task_params['envs']
    inputs = job_params['inputs'] | task_params['inputs']
    outputs = job_params['outputs'] | task_params['outputs']
    mounts = job_params['mounts']

    prepare_env = self._get_prepare_env(
        script, task_view, inputs, outputs, mounts, _DATA_MOUNT_POINT
    )
    localization_env = self._get_localization_env(
        inputs, user_project, _DATA_MOUNT_POINT
    )
    user_environment = self._build_user_environment(
        envs, inputs, outputs, mounts, _DATA_MOUNT_POINT
    )
    delocalization_env = self._get_delocalization_env(
        outputs, user_project, _DATA_MOUNT_POINT
    )
    # This merges all the envs into one dict. Need to use this syntax because
    # of python3.6. In python3.9 we'd prefer to use | operator.
    all_env = {
        **prepare_env,
        **localization_env,
        **user_environment,
        **delocalization_env,
    }
    return all_env

  def submit_job(
      self,
      job_descriptor: job_model.JobDescriptor,
      skip_if_output_present: bool,
  ) -> Dict[str, any]:
    # Validate task data and resources.
    param_util.validate_submit_args_or_fail(
        job_descriptor,
        provider_name=_PROVIDER_NAME,
        input_providers=_SUPPORTED_INPUT_PROVIDERS,
        output_providers=_SUPPORTED_OUTPUT_PROVIDERS,
        logging_providers=_SUPPORTED_LOGGING_PROVIDERS,
    )

    # Prepare and submit jobs.
    launched_tasks = []
    requests = []
    job_id = job_descriptor.job_metadata['job-id']
    # Instead of creating one job per task, create one job with several tasks.
    # We also need to create a list of environments per task. The length of this
    # list determines how many tasks are in the job, and is specified in the
    # TaskGroup's task_count field.
    envs = []
    for task_view in job_model.task_view_generator(job_descriptor):
      env = self._create_env_for_task(task_view)
      envs.append(google_batch_operations.build_environment(env))

    request = self._create_batch_request(job_descriptor, job_id, envs)
    if self._dry_run:
      requests.append(request)
    else:
      # task_id = client.create_job(request=request)
      task_id = self._submit_batch_job(request)
      launched_tasks.append(task_id)
    # If this is a dry-run, emit all the pipeline request objects
    if self._dry_run:
      print(
          json.dumps(requests, indent=2, sort_keys=True, separators=(',', ': '))
      )
    return {
        'job-id': job_id,
        'user-id': job_descriptor.job_metadata.get('user-id'),
        'task-id': [task_id for task_id in launched_tasks if task_id],
    }

  def delete_jobs(
      self,
      user_ids,
      job_ids,
      task_ids,
      labels,
      create_time_min=None,
      create_time_max=None,
  ):
    """Kills the operations associated with the specified job or job.task.

    Args:
      user_ids: List of user ids who "own" the job(s) to cancel.
      job_ids: List of job_ids to cancel.
      task_ids: List of task-ids to cancel.
      labels: List of LabelParam, each must match the job(s) to be canceled.
      create_time_min: a timezone-aware datetime value for the earliest create
        time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
        create time of a task, inclusive.

    Returns:
      A list of tasks canceled and a list of error messages.
    """
    # Look up the job(s)
    tasks = list(
        self.lookup_job_tasks(
            {'RUNNING'},
            user_ids=user_ids,
            job_ids=job_ids,
            task_ids=task_ids,
            labels=labels,
            create_time_min=create_time_min,
            create_time_max=create_time_max,
        )
    )

    print('Found %d tasks to delete.' % len(tasks))
    return google_base.cancel(
        self._batch_handler_def(), self._operations_cancel_api_def(), tasks
    )

  def lookup_job_tasks(
      self,
      statuses: Set[str],
      user_ids=None,
      job_ids=None,
      job_names=None,
      task_ids=None,
      task_attempts=None,
      labels=None,
      create_time_min=None,
      create_time_max=None,
      max_tasks=0,
      page_size=0,
  ):
    client = batch_v1.BatchServiceClient()
    # TODO: Batch API has no 'done' filter like lifesciences API.
    # Need to figure out how to filter for jobs that are completed.
    empty_statuses = set()
    ops_filter = self._build_query_filter(
        empty_statuses,
        user_ids,
        job_ids,
        job_names,
        task_ids,
        task_attempts,
        labels,
        create_time_min,
        create_time_max,
    )
    # Initialize request argument(s)
    request = batch_v1.ListJobsRequest(
        parent=f'projects/{self._project}/locations/{self._location}',
        filter=ops_filter,
    )

    # Make the request
    response = client.list_jobs(request=request)
    for page in response:
      yield GoogleBatchOperation(page)

  def get_tasks_completion_messages(self, tasks):
    # TODO: This needs to return a list of error messages for each task
    pass
