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
import operator
import os
import re
import sys
import textwrap
from typing import Dict, List, Set

from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util
from . import base
from . import google_base
from . import google_batch_operations
from . import google_custom_machine
from . import google_utils

# pylint: disable=g-import-not-at-top
try:
  from google.cloud import batch_v1
except ImportError:
  # TODO: Remove conditional import when batch library is available
  from . import batch_dummy as batch_v1
# pylint: enable=g-import-not-at-top
_PROVIDER_NAME = 'google-batch'
# Index of the prepare action in the runnable list
_PREPARE_INDEX = 1

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

# These are visible to the user task; not yet documented, as we'd *like* to
# find a way to have them visible only to the logging tasks.
_BATCH_LOG_DIR = f'{_VOLUME_MOUNT_POINT}/.logging'
_LOGGING_DIR = f'{_DATA_MOUNT_POINT}/.logging'

_LOG_FILTER_VAR = '_LOG_FILTER_REPR'
_LOG_FILTER_SCRIPT_PATH = f'{_DATA_MOUNT_POINT}/.log_filter_script.py'

# _LOG_FILTER_PYTHON is a block of Python code to execute in both the
# "continuous_logging" and "final_logging" tasks.
#
# Batch API will eventually create three log files in the _BATCH_LOG_FILE_PATH
# directory. They are named:
#
# - output-*.log
# - stdout-*.log
# - stderr-*.log
#
# We will be creating a "staging" location for each of these files.
#
# If any of the batch log files don't exist, touch the associated staging file.
#
# If the output batch log file exists, copy it directly to the staging
# location.
#
# If the stdout/stderr batch log files exist, copy them to their staging
# location. Then filter it so that only user-action logs exist and the prefixes
# are removed. The prefixes look something like:
# [batch_task_logs]<datetime> ERROR:
#   [task_id:task/<job_uid>,runnable_index:<action_number>]

# pylint: disable=anomalous-backslash-in-string
_LOG_FILTER_PYTHON = textwrap.dedent(r"""
import fileinput
import glob
import re
import shutil
import sys
from pathlib import Path

LOGGING_DIR = sys.argv[1]
LOG_FILE_PATH = sys.argv[2]
STDOUT_FILE_PATH = sys.argv[3]
STDERR_FILE_PATH = sys.argv[4]
USER_TASK = sys.argv[5]

def filter_log_file(staging_path: str, stream_string: str):
  # Replaces lines in file inplace
  for line in fileinput.input(staging_path, inplace=True):
    re_search_string = fr"^\[batch_task_logs\].*{stream_string}: \[task_id:task\/.*runnable_index:{USER_TASK}] (.*)"
    match = re.search(re_search_string, line)
    if match:
      modified_line = match.group(1)
      print(modified_line)

def copy_log_to_staging(glob_str: str, staging_path: str, filter_str: str = None):
  # Check if log files exist, and copy to their staging location
  matching_files = list(Path(LOGGING_DIR).glob(glob_str))
  if matching_files:
    assert(len(matching_files) == 1)
    shutil.copy(matching_files[0], staging_path)
    if filter_str:
      filter_log_file(staging_path, filter_str)
  else:
    Path(staging_path).touch()

# We know the log file is named output-<job-uid>.log
# and the stdout/stderr files are named stdout-<job-uid>.log and
# stderr-<job-uid>.log
copy_log_to_staging("output-*.log", LOG_FILE_PATH)
copy_log_to_staging("stdout-*.log", STDOUT_FILE_PATH, filter_str="INFO")
copy_log_to_staging("stderr-*.log", STDERR_FILE_PATH, filter_str="ERROR")
""")
# pylint: enable=anomalous-backslash-in-string

_LOG_CP = textwrap.dedent("""
  python3 "{log_filter_script_path}" \
      "${{LOGGING_DIR}}" \
      "${{LOGGING_DIR}}/log.txt" \
      "${{LOGGING_DIR}}/stdout.txt" \
      "${{LOGGING_DIR}}/stderr.txt" \
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

  while [[ ! -e "${{LOGGING_DIR}}/.stop_logging" ]]; do
    {log_cp}

    sleep "{log_interval}"
  done
""")


_EVENT_REGEX_MAP = {
    'scheduled': re.compile('^Job state is set from QUEUED to SCHEDULED'),
    'start': re.compile('^Job state is set from SCHEDULED to RUNNING'),
    'ok': re.compile('^Job state is set from RUNNING to SUCCEEDED'),
    'fail': re.compile('^Job state is set from .+? to FAILED'),
    'cancellation-in-progress': re.compile(
        '^Job state is set from .+? to CANCELLATION_IN_PROGRESS'
    ),
    'canceled': re.compile('^Job state is set from .+? to CANCELLED'),
}


class GoogleBatchEventMap(object):
  """Helper for extracing a set of normalized, filtered operation events."""

  def __init__(self, op: batch_v1.types.Job):
    self._op = op

  def get_filtered_normalized_events(self):
    """Map and filter the batch API events down to events of interest.

    Returns:
      A list of maps containing the normalized, filtered events.
    """
    events = {}
    for event in google_batch_operations.get_status_events(self._op):
      mapped, _ = self._map(event)
      name = mapped['name']

      events[name] = mapped

    return sorted(list(events.values()), key=operator.itemgetter('event-time'))

  def _map(self, event):
    """Extract elements from a Batch status event and map to a named event."""
    description = event.description
    event_time = event.event_time.rfc3339()

    for name, regex in _EVENT_REGEX_MAP.items():
      match = regex.match(description)
      if match:
        return {'name': name, 'event-time': event_time}, match

    return {'name': description, 'event-time': event_time}, None


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
    # We only need the env for the prepare action (runnable) here.
    env = google_batch_operations.get_environment(self._op, _PREPARE_INDEX)
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
      value = {
          'boot-disk-size': google_batch_operations.get_boot_disk_size(
              self._op
          ),
          'disk-size': google_batch_operations.get_disk_size(self._op),
          'disk-type': google_batch_operations.get_disk_type(self._op),
          'machine-type': google_batch_operations.get_machine_type(self._op),
          'regions': google_batch_operations.get_regions(self._op),
          'zones': google_batch_operations.get_zones(self._op),
          'preemptible': google_batch_operations.get_preemptible(self._op),
      }
    elif field == 'events':
      value = GoogleBatchEventMap(self._op).get_filtered_normalized_events()
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
      msg, _, _ = self._operation_status_message()
      value = msg
    elif field == 'status-detail':
      # As much detail as we can reasonably get from the operation
      msg, _, detail = self._operation_status_message()
      if detail:
        msg = detail
      value = msg
    else:
      raise ValueError(f'Unsupported field: "{field}"')

    return value if value else default

  def _try_op_to_script_body(self):
    # We only need the env for the prepare action (runnable) here.
    env = google_batch_operations.get_environment(self._op, _PREPARE_INDEX)
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
    if google_batch_operations.is_canceled(self._op):
      return 'CANCELED'
    if google_batch_operations.is_failed(self._op):
      return 'FAILURE'

    raise ValueError(
        'Status for operation {} could not be determined'.format(
            self._op['name']
        )
    )

  def _operation_status_message(self):
    """Returns the most relevant status string and failed action.

    This string is meant for display only.

    Returns:
      A triple of:
      - printable status message
      - the action that failed (if any)
      - a detail message (if available)
    """
    msg = ''
    action = None
    detail = None
    status_events = google_batch_operations.get_status_events(self._op)
    if not google_batch_operations.is_done(self._op):
      msg = 'RUNNING'
    elif google_batch_operations.is_success(self._op):
      msg = 'SUCCESS'
    elif google_batch_operations.is_canceled(self._op):
      msg = 'CANCELED'
    elif google_batch_operations.is_failed(self._op):
      msg = 'FAILURE'

    if status_events:
      detail = status_events[-1].description
    return msg, action, detail


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
    storage_service = dsub_util.get_storage_service(credentials=credentials)

    self._dry_run = dry_run
    self._location = location
    self._project = project
    self._storage_service = storage_service

  def _batch_handler_def(self):
    return GoogleBatchBatchHandler

  def _operations_cancel_api_def(self):
    return batch_v1.BatchServiceClient().cancel_job

  def _get_provisioning_model(self, task_resources):
    if task_resources.preemptible:
      return batch_v1.AllocationPolicy.ProvisioningModel.SPOT
    else:
      return batch_v1.AllocationPolicy.ProvisioningModel.STANDARD

  def _get_batch_job_regions(self, regions, zones) -> List[str]:
    """Returns the list of regions and zones to use for a Batch Job request.

    If neither regions nor zones were specified for the Job, then use the
    Batch Job API location as the default region.

    Regions need to be prefixed with "regions/" and zones need to be prefixed
    with "zones/" as documented in
    https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#LocationPolicy

    Args:
      regions (str): A space separated list of regions to use for the Job.
      zones (str): A space separated list of zones to use for the Job.
    """
    if regions:
      regions = [f'regions/{region}' for region in regions]
    if zones:
      zones = [f'zones/{zone}' for zone in zones]
    if not regions and not zones:
      return [f'regions/{self._location}']
    return (regions or []) + (zones or [])

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

  def _format_batch_job_id(self, task_metadata, job_metadata) -> str:
    # Each dsub task is submitted as its own Batch API job, so we
    # append the dsub task-id and task-attempt to the job-id for the
    # batch job ID.
    # For single-task dsub jobs, there is no task-id, so use 0.
    # Use a '-' character as the delimeter because Batch API job ID
    # must match regex ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
    task_id = task_metadata.get('task-id') or 0
    task_attempt = task_metadata.get('task-attempt') or 0
    batch_job_id = job_metadata.get('job-id')
    return f'{batch_job_id}-{task_id}-{task_attempt}'

  def _get_gcs_volumes(self, mounts) -> List[batch_v1.types.Volume]:
    # Return a list of GCS volumes for the Batch Job request.
    gcs_volumes = []
    for gcs_mount in param_util.get_gcs_mounts(mounts):
      mount_path = os.path.join(_VOLUME_MOUNT_POINT, gcs_mount.docker_path)
      # Normalize mount path because API does not allow trailing slashes
      normalized_mount_path = os.path.normpath(mount_path)
      gcs_volume = google_batch_operations.build_gcs_volume(
          gcs_mount.value[len('gs://') :], normalized_mount_path, ['-o ro']
      )
      gcs_volumes.append(gcs_volume)
    return gcs_volumes

  def _get_gcs_volumes_for_user_command(self, mounts) -> List[str]:
    # Return a list of GCS volumes to be included with the
    # user-command runnable
    user_command_volumes = []
    for gcs_mount in param_util.get_gcs_mounts(mounts):
      volume_mount_point = os.path.normpath(
          os.path.join(_VOLUME_MOUNT_POINT, gcs_mount.docker_path)
      )
      data_mount_point = os.path.normpath(
          os.path.join(_DATA_MOUNT_POINT, gcs_mount.docker_path)
      )
      user_command_volumes.append(f'{volume_mount_point}:{data_mount_point}')
    return user_command_volumes

  def _create_batch_request(
      self,
      task_view: job_model.JobDescriptor,
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
        log_cp=_LOG_CP.format(
            log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
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
        log_cp=_LOG_CP.format(
            log_filter_script_path=_LOG_FILTER_SCRIPT_PATH,
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

    envs = job_params['envs'] | task_params['envs']
    inputs = job_params['inputs'] | task_params['inputs']
    outputs = job_params['outputs'] | task_params['outputs']
    mounts = job_params['mounts']
    gcs_volumes = self._get_gcs_volumes(mounts)

    prepare_env = google_batch_operations.build_environment(
        self._get_prepare_env(
            script, task_view, inputs, outputs, mounts, _DATA_MOUNT_POINT
        )
    )
    localization_env = google_batch_operations.build_environment(
        self._get_localization_env(inputs, user_project, _DATA_MOUNT_POINT)
    )
    user_environment = google_batch_operations.build_environment(
        self._build_user_environment(
            envs, inputs, outputs, mounts, _DATA_MOUNT_POINT
        )
    )
    delocalization_env = google_batch_operations.build_environment(
        self._get_delocalization_env(outputs, user_project, _DATA_MOUNT_POINT)
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
            options=None
        )
    )

    runnables.append(
        # prepare
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=prepare_env,
            entrypoint='/bin/bash',
            volumes=[f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}'],
            commands=['-c', prepare_command],
            options=None
        )
    )

    runnables.append(
        # localization
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=localization_env,
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
            options=None
        )
    )

    user_command_volumes = [f'{_VOLUME_MOUNT_POINT}:{_DATA_MOUNT_POINT}']
    for gcs_volume in self._get_gcs_volumes_for_user_command(mounts):
      user_command_volumes.append(gcs_volume)
    # Add --gpus all option for GPU-enabled containers
    container_options = '--gpus all' if job_resources.accelerator_type and job_resources.accelerator_type.startswith('nvidia') else None
    runnables.append(
        # user-command
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=job_resources.image,
            environment=user_environment,
            entrypoint='/usr/bin/env',
            volumes=user_command_volumes,
            commands=[
                'bash',
                '-c',
                google_utils.USER_CMD.format(
                    tmp_dir=_TMP_DIR,
                    working_dir=_WORKING_DIR,
                    user_script=script_path,
                ),
            ],
            options=container_options,
        )
    )

    runnables.append(
        # delocalization
        google_batch_operations.build_runnable(
            run_in_background=False,
            always_run=False,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=delocalization_env,
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
            options=None
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
            options=None
        ),
    )

    # Prepare the VM (resources) configuration. The InstancePolicy describes an
    # instance type and resources attached to each VM. The AllocationPolicy
    # describes when, where, and how compute resources should be allocated
    # for the Job.
    boot_disk_size = (
        job_resources.boot_disk_size if job_resources.boot_disk_size else 0
    )
    # Determine boot disk image: use user-specified value, or default to batch-debian for GPU jobs
    if job_resources.boot_disk_image:
      boot_disk_image = job_resources.boot_disk_image
    elif job_resources.accelerator_type and job_resources.accelerator_type.startswith('nvidia'):
      boot_disk_image = 'batch-debian'
    else:
      boot_disk_image = None

    boot_disk = google_batch_operations.build_persistent_disk(
        size_gb=max(boot_disk_size, job_model.LARGE_BOOT_DISK_SIZE),
        disk_type=job_model.DEFAULT_DISK_TYPE,
        image=boot_disk_image,
    )
    disk = google_batch_operations.build_persistent_disk(
        size_gb=job_resources.disk_size,
        disk_type=job_resources.disk_type or job_model.DEFAULT_DISK_TYPE,
        image=None
    )
    attached_disk = google_batch_operations.build_attached_disk(
        disk=disk, device_name=google_utils.DATA_DISK_NAME
    )

    if job_resources.machine_type:
      machine_type = job_resources.machine_type
    elif job_resources.min_cores or job_resources.min_ram:
      machine_type = (
          google_custom_machine.GoogleCustomMachine.build_machine_type(
              job_resources.min_cores, job_resources.min_ram
          )
      )
    else:
      machine_type = job_model.DEFAULT_MACHINE_TYPE

    instance_policy = google_batch_operations.build_instance_policy(
        boot_disk=boot_disk,
        disks=attached_disk,
        machine_type=machine_type,
        accelerators=google_batch_operations.build_accelerators(
            accelerator_type=job_resources.accelerator_type,
            accelerator_count=job_resources.accelerator_count,
        ),
        provisioning_model=self._get_provisioning_model(task_resources),
    )

    # Determine whether to install GPU drivers: use user-specified value, or default to True for GPU jobs
    if job_resources.install_gpu_drivers is not None:
      install_gpu_drivers = job_resources.install_gpu_drivers
    else:
      install_gpu_drivers = job_resources.accelerator_type is not None

    ipt = google_batch_operations.build_instance_policy_or_template(
        instance_policy=instance_policy,
        install_gpu_drivers=install_gpu_drivers,
    )

    if job_resources.service_account:
      scopes = job_resources.scopes or google_base.DEFAULT_SCOPES
      service_account = google_batch_operations.build_service_account(
          service_account_email=job_resources.service_account, scopes=scopes
      )
    else:
      service_account = None

    network_policy = google_batch_operations.build_network_policy(
        network=job_resources.network,
        subnetwork=job_resources.subnetwork,
        no_external_ip_address=job_resources.use_private_address,
    )

    location_policy = google_batch_operations.build_location_policy(
        allowed_locations=self._get_batch_job_regions(
            regions=job_resources.regions, zones=job_resources.zones
        ),
    )

    allocation_policy = google_batch_operations.build_allocation_policy(
        ipts=[ipt],
        service_account=service_account,
        network_policy=network_policy,
        location_policy=location_policy,
    )

    logs_policy = google_batch_operations.build_logs_policy(
        # Explicitly end the logging path with a slash.
        # This will prompt Batch API to create the log, stdout, and stderr
        # files in the specified directory.
        batch_v1.LogsPolicy.Destination.PATH,
        _BATCH_LOG_DIR + '/',
    )

    # Bring together the task definition(s) and build the Job request.
    task_spec = google_batch_operations.build_task_spec(
        runnables=runnables, volumes=([datadisk_volume] + gcs_volumes), max_run_duration=job_resources.timeout
    )
    task_group = google_batch_operations.build_task_group(
        task_spec, task_count=1, task_count_per_node=1
    )

    job = google_batch_operations.build_job(
        [task_group], allocation_policy, labels, logs_policy
    )

    batch_job_id = self._format_batch_job_id(task_metadata, job_metadata)

    job_request = batch_v1.CreateJobRequest(
        parent=f'projects/{self._project}/locations/{self._location}',
        job=job,
        job_id=batch_job_id,
    )
    # pylint: enable=line-too-long
    return job_request

  def _submit_batch_job(self, request) -> str:
    client = batch_v1.BatchServiceClient()
    job_response = client.create_job(request=request)
    op = GoogleBatchOperation(job_response)
    print(f'Provider internal-id (operation): {job_response.name}')
    return op.get_field('task-id')

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

    for task_view in job_model.task_view_generator(job_descriptor):

      job_params = task_view.job_params
      task_params = task_view.task_descriptors[0].task_params

      outputs = job_params['outputs'] | task_params['outputs']
      if skip_if_output_present:
        # check whether the output's already there
        if dsub_util.outputs_are_present(outputs, self._storage_service):
          print('Skipping task because its outputs are present')
          continue

      request = self._create_batch_request(task_view)
      if self._dry_run:
        requests.append(request)
      else:
        task_id = self._submit_batch_job(request)
        launched_tasks.append(task_id)

    # If this is a dry-run, emit all the batch request objects
    if self._dry_run:
      # Each request is a google.cloud.batch_v1.types.batch.CreateJobRequest
      # object. The __repr__ method for this object outputs something that
      # closely resembles yaml, but can't actually be serialized into yaml.
      # Ideally, we could serialize these request objects to yaml or json.
      print(requests)

    if not requests and not launched_tasks:
      return {'job-id': dsub_util.NO_JOB}

    return {
        'job-id': job_descriptor.job_metadata['job-id'],
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
    ops_filter = self._build_query_filter(
        statuses,
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
    # Sort the operations by create-time to match sort of other providers
    operations = [GoogleBatchOperation(page) for page in response]
    operations.sort(key=lambda op: op.get_field('create-time'), reverse=True)
    for op in operations:
      yield op

  def get_tasks_completion_messages(self, tasks):
    # TODO: This needs to return a list of error messages for each task
    pass