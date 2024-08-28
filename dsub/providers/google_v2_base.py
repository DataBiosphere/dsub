# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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

This module serves as the base class for the google-cls-v2 provider. It also
used to be the base class for the now gone google-v2 provider.
The APIs they were based on were very similar and benefited from sharing code.
"""
import ast
import json
import operator
import os
import re
import sys
import textwrap

from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util
from . import base
from . import google_base
from . import google_custom_machine
from . import google_utils
from . import google_v2_operations
from . import google_v2_pipelines
from . import google_v2_versions

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS

# This image is for an optional ssh container.
_SSH_IMAGE = 'gcr.io/cloud-genomics-pipelines/tools'
_DEFAULT_SSH_PORT = 22

# This image is for an optional mount on a bucket using GCS Fuse
_GCSFUSE_IMAGE = 'gcr.io/cloud-genomics-pipelines/gcsfuse:latest'

# The logging in v2alpha1 is different than in v1alpha2.
# v1alpha2 would provide:
#   [your_path].log: Logging of the on-instance "controller" code that the
#                    Google Pipelines API ran.
#   [your_path]-stdout.log: stdout logging for the user command
#   [your_path]-stderr.log: stderr logging for the user command
#
# v2alpha1 allows provides:
#   /google/logs/output: <all container output>
#   /google/logs/action/<action-number>/stdout: stdout logging of the action
#   /google/logs/action/<action-number>/stderr: stderr logging of the action
#
# We explicitly copy off the logs, simulating v1alpha2 behavior.
# If the logging commands fail, there is currently no useful information in
# the operation to indicate why it failed.

_LOG_CP_CMD = textwrap.dedent("""\
  mkdir -p /tmp/{logging_action}
  log_cp /google/logs/action/{user_action}/stdout "${{STDOUT_PATH}}" /tmp/{logging_action} "true" "${{USER_PROJECT}}" &
  STDOUT_PID=$!
  log_cp /google/logs/action/{user_action}/stderr "${{STDERR_PATH}}" /tmp/{logging_action} "true" "${{USER_PROJECT}}" &
  STDERR_PID=$!

  log_cp /google/logs/output "${{LOGGING_PATH}}" /tmp/{logging_action} "false" "${{USER_PROJECT}}" &
  LOG_PID=$!

  wait "${{STDOUT_PID}}"
  wait "${{STDERR_PID}}"
  wait "${{LOG_PID}}"
""")

_FINAL_LOGGING_CMD = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  {log_msg_fn}
  {log_cp_fn}
  {log_cp_cmd}
""")

# Keep logging until the final logging action starts
_CONTINUOUS_LOGGING_CMD = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  {log_msg_fn}
  {log_cp_fn}

  while [[ ! -d /google/logs/action/{final_logging_action} ]]; do
    {log_cp_cmd}

    sleep {log_interval}
  done
""")

_ACTION_LOGGING = 'logging'
_ACTION_PREPARE = 'prepare'
_ACTION_LOCALIZATION = 'localization'
_ACTION_USER_COMMAND = 'user-command'
_ACTION_DELOCALIZATION = 'delocalization'
_ACTION_FINAL_LOGGING = 'final_logging'
_FILTER_ACTIONS = [_ACTION_LOGGING, _ACTION_PREPARE, _ACTION_FINAL_LOGGING]
_FILTERED_EVENT_REGEXES = [
    re.compile('^Started running "({})"$'.format('|'.join(_FILTER_ACTIONS))),
    re.compile('Stopped pulling'),
    re.compile('Stopped running'),
    # An error causes two events, one we capture and one is filtered out.
    re.compile('Execution failed: action 4: unexpected exit status [\\d]{1,4}')
]

_ABORT_REGEX = re.compile('The operation was cancelled')
_FAIL_REGEX = re.compile(
    '^Unexpected exit status [\\d]{1,4} while running "user-command"$')
_EVENT_REGEX_MAP = {
    'start': re.compile('^Worker ".*" assigned in ".*".*$'),
    'pulling-image': re.compile('^Started pulling "(.*)"$'),
    'localizing-files': re.compile('^Started running "localization"$'),
    'running-docker': re.compile('^Started running "user-command"$'),
    'delocalizing-files': re.compile('^Started running "delocalization"$'),
    'ok': re.compile('^Worker released$'),
    'fail': _FAIL_REGEX,
    'canceled': _ABORT_REGEX,
}

# Mount point for the data disk in the user's Docker container
_DATA_MOUNT_POINT = '/mnt/data'

_SCRIPT_DIR = f'{_DATA_MOUNT_POINT}/script'
_TMP_DIR = f'{_DATA_MOUNT_POINT}/tmp'
_WORKING_DIR = f'{_DATA_MOUNT_POINT}/workingdir'


class GoogleV2EventMap(object):
  """Helper for extracing a set of normalized, filtered operation events."""

  def __init__(self, op):
    self._op = op

  def get_filtered_normalized_events(self):
    """Filter the granular v2 events down to events of interest.

    Filter through the large number of granular events returned by the
    pipelines API, and extract only those that are interesting to a user. This
    is implemented by filtering out events which are known to be uninteresting
    (i.e. the default actions run for every job) and by explicitly matching
    specific events which are interesting and mapping those to v1 style naming.

    Events which are not whitelisted or blacklisted will still be output,
    meaning any events which are added in the future won't be masked.
    We don't want to suppress display of events that we don't recognize.
    They may be important.

    Returns:
      A list of maps containing the normalized, filtered events.
    """
    # Need the user-image to look for the right "pulling image" event
    user_image = google_v2_operations.get_action_image(self._op,
                                                       _ACTION_USER_COMMAND)

    # Only create an "ok" event for operations with SUCCESS status.
    need_ok = google_v2_operations.is_success(self._op)

    # Events are keyed by name for easier deletion.
    events = {}

    # Events are assumed to be ordered by timestamp (newest to oldest).
    for event in google_v2_operations.get_events(self._op):
      if self._filter(event):
        continue

      mapped, match = self._map(event)
      name = mapped['name']

      if name == 'ok':
        # If we want the "ok" event, we grab the first (most recent).
        if not need_ok or 'ok' in events:
          continue

      if name == 'pulling-image':
        if match and match.group(1) != user_image:
          continue

      events[name] = mapped

    return sorted(list(events.values()), key=operator.itemgetter('start-time'))

  def _map(self, event):
    """Extract elements from an operation event and map to a named event."""
    description = event.get('description', '')
    start_time = google_base.parse_rfc3339_utc_string(
        event.get('timestamp', ''))

    for name, regex in _EVENT_REGEX_MAP.items():
      match = regex.match(description)
      if match:
        return {'name': name, 'start-time': start_time}, match

    return {'name': description, 'start-time': start_time}, None

  def _filter(self, event):
    for regex in _FILTERED_EVENT_REGEXES:
      if regex.match(event.get('description', '')):
        return True

    return False


class GoogleV2BatchHandler(object):
  """Implement the HttpBatch interface to enable simple serial batches."""

  # The v2alpha1 batch endpoint is not currently implemented.
  # When it is, this can be replaced by service.new_batch_http_request.

  def __init__(self, callback):
    self._cancel_list = []
    self._response_handler = callback

  def add(self, cancel_fn, request_id):
    self._cancel_list.append((request_id, cancel_fn))

  def execute(self):
    for (request_id, cancel_fn) in self._cancel_list:
      response = None
      exception = None
      try:
        response = cancel_fn.execute()
      except:  # pylint: disable=bare-except
        exception = sys.exc_info()[1]

      self._response_handler(request_id, response, exception)


class GoogleV2JobProviderBase(google_utils.GoogleJobProviderBase):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(self, provider_name, api_version, credentials, project, dry_run):
    google_v2_pipelines.set_api_version(api_version)
    google_v2_operations.set_api_version(api_version)

    service = google_base.setup_service(
        google_v2_versions.get_api_name(api_version), api_version, credentials)

    storage_service = dsub_util.get_storage_service(credentials=credentials)

    self._provider_name = provider_name
    self._service = service
    self._project = project
    self._dry_run = dry_run
    self._storage_service = storage_service

  def _get_pipeline_regions(self, regions, zones):
    """Returns the list of regions to use for a pipeline request."""
    raise NotImplementedError('Derived class must implement this function')

  def _pipelines_run_api(self, request):
    """Executes the provider-specific pipelines.run() API."""
    raise NotImplementedError('Derived class must implement this function')

  def _operations_list_api(self, ops_filter, page_token, page_size):
    """Executes the provider-specific operaitons.list() API."""
    raise NotImplementedError('Derived class must implement this function')

  def _operations_cancel_api_def(self):
    """Returns a function object for the provider-specific cancel API."""
    raise NotImplementedError('Derived class must implement this function')

  def _batch_handler_def(self):
    """Returns a function object for the provider-specific batch handler."""
    raise NotImplementedError('Derived class must implement this function')

  def _get_logging_env(self, logging_uri, user_project):
    """Returns the environment for actions that copy logging files."""
    if not logging_uri.endswith('.log'):
      raise ValueError('Logging URI must end in ".log": {}'.format(logging_uri))

    logging_prefix = logging_uri[:-len('.log')]
    return {
        'LOGGING_PATH': '{}.log'.format(logging_prefix),
        'STDOUT_PATH': '{}-stdout.log'.format(logging_prefix),
        'STDERR_PATH': '{}-stderr.log'.format(logging_prefix),
        'USER_PROJECT': user_project,
    }

  def _get_mount_actions(self, mounts, mnt_datadisk, user_project):
    """Returns a list of two actions per gcs bucket to mount."""
    actions_to_add = []
    for mount in mounts:
      bucket = mount.value[len('gs://'):]
      mount_path = mount.docker_path

      mount_command = (
          ['--billing-project', user_project] if user_project else []
      )
      mount_command.extend([
          '--implicit-dirs',
          '--foreground',
          '-o ro',
          bucket,
          os.path.join(_DATA_MOUNT_POINT, mount_path),
      ])

      actions_to_add.extend([
          google_v2_pipelines.build_action(
              name='mount-{}'.format(bucket),
              enable_fuse=True,
              run_in_background=True,
              image_uri=_GCSFUSE_IMAGE,
              mounts=[mnt_datadisk],
              commands=mount_command,
          ),
          google_v2_pipelines.build_action(
              name='mount-wait-{}'.format(bucket),
              enable_fuse=True,
              image_uri=_GCSFUSE_IMAGE,
              mounts=[mnt_datadisk],
              commands=[
                  'wait',
                  os.path.join(_DATA_MOUNT_POINT, mount_path),
              ],
          ),
      ])
    return actions_to_add

  def _build_pipeline_request(self, task_view):
    """Returns a Pipeline objects for the task."""
    job_metadata = task_view.job_metadata
    job_params = task_view.job_params
    job_resources = task_view.job_resources
    task_metadata = task_view.task_descriptors[0].task_metadata
    task_params = task_view.task_descriptors[0].task_params
    task_resources = task_view.task_descriptors[0].task_resources

    # Set up VM-specific variables
    mnt_datadisk = google_v2_pipelines.build_mount(
        disk=google_utils.DATA_DISK_NAME,
        path=_DATA_MOUNT_POINT,
        read_only=False)
    scopes = job_resources.scopes or google_base.DEFAULT_SCOPES

    # Set up the task labels
    labels = {
        label.name: label.value if label.value else '' for label in
        google_base.build_pipeline_labels(job_metadata, task_metadata)
        | job_params['labels'] | task_params['labels']
    }

    # Set local variables for the core pipeline values
    script = task_view.job_metadata['script']
    user_project = task_view.job_metadata['user-project'] or ''

    envs = job_params['envs'] | task_params['envs']
    inputs = job_params['inputs'] | task_params['inputs']
    outputs = job_params['outputs'] | task_params['outputs']
    mounts = job_params['mounts']
    gcs_mounts = param_util.get_gcs_mounts(mounts)

    persistent_disk_mount_params = param_util.get_persistent_disk_mounts(mounts)
    existing_disk_mount_params = param_util.get_existing_disk_mounts(mounts)

    # pylint: disable=g-complex-comprehension
    persistent_disks = [
        google_v2_pipelines.build_volume_persistent_disk(
            volume=disk.name.replace('_', '-'),  # Underscores not allowed
            disk=google_v2_pipelines.build_persistent_disk(
                size_gb=disk.disk_size or job_model.DEFAULT_MOUNTED_DISK_SIZE,
                source_image=disk.value,
                disk_type=disk.disk_type or job_model.DEFAULT_DISK_TYPE))
        for disk in persistent_disk_mount_params
    ]
    persistent_disk_mounts = [
        google_v2_pipelines.build_mount(
            disk=persistent_disk.get('volume'),
            path=os.path.join(_DATA_MOUNT_POINT,
                              persistent_disk_mount_param.docker_path),
            read_only=True)
        for persistent_disk, persistent_disk_mount_param in zip(
            persistent_disks, persistent_disk_mount_params)
    ]
    # pylint: enable=g-complex-comprehension

    # pylint: disable=g-complex-comprehension
    existing_disks = [
        google_v2_pipelines.build_volume_existing_disk(
            volume=disk.name.replace('_', '-'),  # Underscores not allowed
            disk=google_v2_pipelines.build_existing_disk(disk=disk.value))
        for disk in existing_disk_mount_params
    ]
    existing_disk_mounts = [
        google_v2_pipelines.build_mount(
            disk=existing_disk.get('volume'),
            path=os.path.join(_DATA_MOUNT_POINT,
                              existing_disk_mount_param.docker_path),
            read_only=True) for existing_disk, existing_disk_mount_param in zip(
                existing_disks, existing_disk_mount_params)
    ]
    # pylint: enable=g-complex-comprehension

    # The list of "actions" (1-based) will be:
    #   1- continuous copy of log files off to Cloud Storage
    #   2- prepare the shared mount point (write the user script)
    #   3- localize objects from Cloud Storage to block storage
    #   4- execute user command
    #   5- delocalize objects from block storage to Cloud Storage
    #   6- final copy of log files off to Cloud Storage
    #
    # If the user has requested an SSH server be started, it will be inserted
    # after logging is started, and all subsequent action numbers above will be
    # incremented by 1.
    # If the user has requested to mount one or more buckets, two actions per
    # bucket will be inserted after the prepare step, and all subsequent action
    # numbers will be incremented by the number of actions added.
    #
    # We need to track the action numbers specifically for the user action and
    # the final logging action.
    optional_actions = 0
    if job_resources.ssh:
      optional_actions += 1

    mount_actions = self._get_mount_actions(
        gcs_mounts, mnt_datadisk, user_project
    )
    optional_actions += len(mount_actions)

    user_action = 4 + optional_actions
    final_logging_action = 6 + optional_actions

    # Set up the commands and environment for the logging actions
    final_logging_cmd = _FINAL_LOGGING_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        log_cp_fn=google_utils.LOG_CP_FN,
        log_cp_cmd=_LOG_CP_CMD.format(
            user_action=user_action, logging_action='logging_action'
        ),
    )
    continuous_logging_cmd = _CONTINUOUS_LOGGING_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        log_cp_fn=google_utils.LOG_CP_FN,
        log_cp_cmd=_LOG_CP_CMD.format(
            user_action=user_action, logging_action='continuous_logging_action'
        ),
        final_logging_action=final_logging_action,
        log_interval=job_resources.log_interval or '60s',
    )
    logging_env = self._get_logging_env(task_resources.logging_path.uri,
                                        user_project)

    # Set up command and environments for the prepare, localization, user,
    # and de-localization actions
    script_path = os.path.join(_SCRIPT_DIR, script.name)
    prepare_command = google_utils.PREPARE_CMD.format(
        log_msg_fn=google_utils.LOG_MSG_FN,
        mk_runtime_dirs=google_utils.make_runtime_dirs_command(
            _SCRIPT_DIR, _TMP_DIR, _WORKING_DIR),
        script_var=google_utils.SCRIPT_VARNAME,
        python_decode_script=google_utils.PYTHON_DECODE_SCRIPT,
        script_path=script_path,
        mk_io_dirs=google_utils.MK_IO_DIRS)

    prepare_env = self._get_prepare_env(script, task_view, inputs, outputs,
                                        mounts, _DATA_MOUNT_POINT)
    localization_env = self._get_localization_env(inputs, user_project,
                                                  _DATA_MOUNT_POINT)
    user_environment = self._build_user_environment(envs, inputs, outputs,
                                                    mounts, _DATA_MOUNT_POINT)
    delocalization_env = self._get_delocalization_env(outputs, user_project,
                                                      _DATA_MOUNT_POINT)

    # When --ssh is enabled, run all actions in the same process ID namespace
    pid_namespace = 'shared' if job_resources.ssh else None

    # Build the list of actions
    actions = []
    actions.append(
        google_v2_pipelines.build_action(
            name='logging',
            pid_namespace=pid_namespace,
            run_in_background=True,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=logging_env,
            entrypoint='/bin/bash',
            commands=['-c', continuous_logging_cmd]))

    if job_resources.ssh:
      actions.append(
          google_v2_pipelines.build_action(
              name='ssh',
              pid_namespace=pid_namespace,
              image_uri=_SSH_IMAGE,
              mounts=[mnt_datadisk],
              entrypoint='ssh-server',
              port_mappings={_DEFAULT_SSH_PORT: _DEFAULT_SSH_PORT},
              run_in_background=True))

    actions.append(
        google_v2_pipelines.build_action(
            name='prepare',
            pid_namespace=pid_namespace,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            mounts=[mnt_datadisk],
            environment=prepare_env,
            entrypoint='/bin/bash',
            commands=['-c', prepare_command]),)

    actions.extend(mount_actions)

    actions.extend([
        google_v2_pipelines.build_action(
            name='localization',
            pid_namespace=pid_namespace,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            mounts=[mnt_datadisk],
            environment=localization_env,
            entrypoint='/bin/bash',
            commands=[
                '-c',
                google_utils.LOCALIZATION_CMD.format(
                    log_msg_fn=google_utils.LOG_MSG_FN,
                    recursive_cp_fn=google_utils.GSUTIL_RSYNC_FN,
                    cp_fn=google_utils.GSUTIL_CP_FN,
                    cp_loop=google_utils.LOCALIZATION_LOOP,
                ),
            ],
        ),
        google_v2_pipelines.build_action(
            name='user-command',
            pid_namespace=pid_namespace,
            block_external_network=job_resources.block_external_network,
            image_uri=job_resources.image,
            mounts=[mnt_datadisk]
            + persistent_disk_mounts
            + existing_disk_mounts,
            environment=user_environment,
            entrypoint='/usr/bin/env',
            commands=[
                'bash',
                '-c',
                google_utils.USER_CMD.format(
                    tmp_dir=_TMP_DIR,
                    working_dir=_WORKING_DIR,
                    user_script=script_path,
                ),
            ],
        ),
        google_v2_pipelines.build_action(
            name='delocalization',
            pid_namespace=pid_namespace,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            mounts=[mnt_datadisk],
            environment=delocalization_env,
            entrypoint='/bin/bash',
            commands=[
                '-c',
                google_utils.LOCALIZATION_CMD.format(
                    log_msg_fn=google_utils.LOG_MSG_FN,
                    recursive_cp_fn=google_utils.GSUTIL_RSYNC_FN,
                    cp_fn=google_utils.GSUTIL_CP_FN,
                    cp_loop=google_utils.DELOCALIZATION_LOOP,
                ),
            ],
        ),
        google_v2_pipelines.build_action(
            name='final_logging',
            pid_namespace=pid_namespace,
            always_run=True,
            image_uri=google_utils.CLOUD_SDK_IMAGE,
            environment=logging_env,
            entrypoint='/bin/bash',
            commands=['-c', final_logging_cmd],
        ),
    ])

    assert len(actions) - 2 == user_action
    assert len(actions) == final_logging_action

    # Prepare the VM (resources) configuration
    volumes = [
        google_v2_pipelines.build_volume_persistent_disk(
            volume=google_utils.DATA_DISK_NAME,
            disk=google_v2_pipelines.build_persistent_disk(
                job_resources.disk_size,
                source_image=None,
                disk_type=job_resources.disk_type
                or job_model.DEFAULT_DISK_TYPE,
            ),
        )
    ]
    volumes.extend(persistent_disks)
    volumes.extend(existing_disks)

    network = google_v2_pipelines.build_network(
        job_resources.network,
        job_resources.subnetwork,
        job_resources.use_private_address,
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
    accelerators = None
    if job_resources.accelerator_type:
      accelerators = [
          google_v2_pipelines.build_accelerator(
              job_resources.accelerator_type, job_resources.accelerator_count
          )
      ]
    service_account = google_v2_pipelines.build_service_account(
        job_resources.service_account or 'default', scopes
    )

    resources = google_v2_pipelines.build_resources(
        self._project,
        self._get_pipeline_regions(job_resources.regions, job_resources.zones),
        google_base.get_zones(job_resources.zones),
        google_v2_pipelines.build_machine(
            network=network,
            machine_type=machine_type,
            # Preemptible comes from task_resources because it may change
            # on retry attempts
            preemptible=task_resources.preemptible,
            service_account=service_account,
            boot_disk_size_gb=job_resources.boot_disk_size,
            volumes=volumes,
            accelerators=accelerators,
            nvidia_driver_version=job_resources.nvidia_driver_version,
            labels=labels,
            cpu_platform=job_resources.cpu_platform,
            enable_stackdriver_monitoring=job_resources
            .enable_stackdriver_monitoring),
    )

    # Build the pipeline request
    pipeline = google_v2_pipelines.build_pipeline(actions, resources, None,
                                                  job_resources.timeout)

    return {'pipeline': pipeline, 'labels': labels}

  def _submit_pipeline(self, request):
    google_base_api = google_base.Api()
    operation = google_base_api.execute(self._pipelines_run_api(request))
    print('Provider internal-id (operation): {}'.format(operation['name']))

    return GoogleOperation(self._provider_name, operation).get_field('task-id')

  def submit_job(self, job_descriptor, skip_if_output_present):
    """Submit the job (or tasks) to be executed.

    Args:
      job_descriptor: all parameters needed to launch all job tasks
      skip_if_output_present: (boolean) if true, skip tasks whose output
        is present (see --skip flag for more explanation).

    Returns:
      A dictionary containing the 'user-id', 'job-id', and 'task-id' list.
      For jobs that are not task array jobs, the task-id list should be empty.

    Raises:
      ValueError: if job resources or task data contain illegal values.
    """
    # Validate task data and resources.
    param_util.validate_submit_args_or_fail(
        job_descriptor,
        provider_name=self._provider_name,
        input_providers=_SUPPORTED_INPUT_PROVIDERS,
        output_providers=_SUPPORTED_OUTPUT_PROVIDERS,
        logging_providers=_SUPPORTED_LOGGING_PROVIDERS)

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

      request = self._build_pipeline_request(task_view)

      if self._dry_run:
        requests.append(request)
      else:
        task_id = self._submit_pipeline(request)
        launched_tasks.append(task_id)

    # If this is a dry-run, emit all the pipeline request objects
    if self._dry_run:
      print(
          json.dumps(
              requests, indent=2, sort_keys=True, separators=(',', ': ')))

    if not requests and not launched_tasks:
      return {'job-id': dsub_util.NO_JOB}

    return {
        'job-id': job_descriptor.job_metadata['job-id'],
        'user-id': job_descriptor.job_metadata['user-id'],
        'task-id': [task_id for task_id in launched_tasks if task_id],
    }

  def get_tasks_completion_messages(self, tasks):
    completion_messages = []
    for task in tasks:
      completion_messages.append(task.error_message())
    return completion_messages

  def _operations_list(self, ops_filter, max_tasks, page_size, page_token):
    """Gets the list of operations for the specified filter.

    Args:
      ops_filter: string filter of operations to return
      max_tasks: the maximum number of job tasks to return or 0 for no limit.
      page_size: the number of operations to requested on each list operation to
        the pipelines API (if 0 or None, the API default is used)
      page_token: page token returned by a previous _operations_list call.

    Returns:
      Operations matching the filter criteria.
    """

    # We are not using the documented default page size of 256,
    # nor allowing for the maximum page size of 2048 as larger page sizes
    # currently cause the operations.list() API to return an error:
    # HttpError 429 ... Resource has been exhausted (e.g. check quota).
    max_page_size = 128

    # Set the page size to the smallest (non-zero) size we can
    page_size = min(sz for sz in [page_size, max_page_size, max_tasks] if sz)

    # Execute operations.list() and return all of the dsub operations
    api = self._operations_list_api(ops_filter, page_token, page_size)
    google_base_api = google_base.Api()
    response = google_base_api.execute(api)

    return [
        GoogleOperation(self._provider_name, op)
        for op in response.get('operations', [])
        if google_v2_operations.is_dsub_operation(op)
    ], response.get('nextPageToken')

  def lookup_job_tasks(self,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       task_attempts=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0,
                       page_size=0):
    """Yields operations based on the input criteria.

    If any of the filters are empty or {'*'}, then no filtering is performed on
    that field. Filtering by both a job id list and job name list is
    unsupported.

    Args:
      statuses: {'*'}, or a list of job status strings to return. Valid
        status strings are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_ids: a set of ids for the user(s) who launched the job.
      job_ids: a set of job ids to return.
      job_names: a set of job names to return.
      task_ids: a set of specific tasks within the specified job(s) to return.
      task_attempts: a list of specific attempts within the specified tasks(s)
        to return.
      labels: a list of LabelParam with user-added labels. All labels must
              match the task being fetched.
      create_time_min: a timezone-aware datetime value for the earliest create
                       time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
                       create time of a task, inclusive.
      max_tasks: the maximum number of job tasks to return or 0 for no limit.
      page_size: the page size to use for each query to the pipelins API.

    Raises:
      ValueError: if both a job id list and a job name list are provided

    Yields:
      Genomics API Operations objects.
    """

    # Build a filter for operations to return
    ops_filter = self._build_query_filter(
        statuses, user_ids, job_ids, job_names, task_ids, task_attempts, labels,
        create_time_min, create_time_max)

    # Execute the operations.list() API to get batches of operations to yield
    page_token = None
    tasks_yielded = 0
    while True:
      # If max_tasks is set, let operations.list() know not to send more than
      # we need.
      max_to_fetch = None
      if max_tasks:
        max_to_fetch = max_tasks - tasks_yielded
      ops, page_token = self._operations_list(ops_filter, max_to_fetch,
                                              page_size, page_token)

      for op in ops:
        yield op
        tasks_yielded += 1

      assert (max_tasks >= tasks_yielded or not max_tasks)
      if not page_token or 0 < max_tasks <= tasks_yielded:
        break

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
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
            create_time_max=create_time_max))

    print('Found %d tasks to delete.' % len(tasks))

    return google_base.cancel(self._batch_handler_def(),
                              self._operations_cancel_api_def(), tasks)


class GoogleOperation(base.Task):
  """Task wrapper around a Pipelines API operation object."""

  def __init__(self, provider_name, operation_data):
    self._provider_name = provider_name
    self._op = operation_data
    self._job_descriptor = self._try_op_to_job_descriptor()

  def raw_task_data(self):
    return self._op

  def _try_op_to_job_descriptor(self):
    # The _META_YAML_REPR field in the 'prepare' action enables reconstructing
    # the original job descriptor.
    # Jobs run while the google-v2 provider was in development will not have
    # the _META_YAML.
    env = google_v2_operations.get_action_environment(self._op, 'prepare')
    if not env:
      return

    meta = env.get(google_utils.META_YAML_VARNAME)
    if not meta:
      return

    return job_model.JobDescriptor.from_yaml(ast.literal_eval(meta))

  def _try_op_to_script_body(self):
    env = google_v2_operations.get_action_environment(self._op, _ACTION_PREPARE)
    if env:
      return ast.literal_eval(env.get(google_utils.SCRIPT_VARNAME))

  def _operation_status(self):
    """Returns the status of this operation.

    Raises:
      ValueError: if the operation status cannot be determined.

    Returns:
      A printable status string (RUNNING, SUCCESS, CANCELED or FAILURE).
    """
    if not google_v2_operations.is_done(self._op):
      return 'RUNNING'
    if google_v2_operations.is_success(self._op):
      return 'SUCCESS'
    if google_v2_operations.is_canceled(self._op):
      return 'CANCELED'
    if google_v2_operations.is_failed(self._op):
      return 'FAILURE'

    raise ValueError('Status for operation {} could not be determined'.format(
        self._op['name']))

  def _operation_status_message(self):
    """Returns the most relevant status string and failed action.

    This string is meant for display only.

    Returns:
      A triple of:
      - printable status message
      - the action that failed (if any)
      - a detail message (if available)
    """
    msg = None
    action = None
    detail = None

    if not google_v2_operations.is_done(self._op):
      last_event = google_v2_operations.get_last_event(self._op)
      if last_event:
        if google_v2_operations.is_worker_assigned_event(last_event):
          msg = 'VM starting (awaiting worker checkin)'
          detail = last_event['description']
        elif google_v2_operations.is_pull_started_event(last_event):
          detail = last_event['description']
          msg = detail.replace('Started pulling', 'Pulling')
        else:
          msg = last_event['description']
          action_id = last_event.get('details', {}).get('actionId')
          if action_id:
            action = google_v2_operations.get_action_by_id(self._op, action_id)
      else:
        msg = 'Pending'

    elif google_v2_operations.is_success(self._op):
      msg = 'Success'

    else:
      # We have a failure condition and want to get the best details of why.

      # For a single failure, we may get multiple failure events.
      # For the Life Sciences v2 provider, events may look like:

      # - description: 'Execution failed: generic::failed_precondition: ...
      #   failed:
      #     cause: 'Execution failed: generic::failed_precondition: while ...
      #     code: FAILED_PRECONDITION
      #   timestamp: '2020-09-28T23:10:09.364365339Z'
      # - description: Unexpected exit status 127 while running "user-command"
      #   timestamp: '2020-09-28T23:10:04.671139036Z'
      #   unexpectedExitStatus:
      #     actionId: 4
      #     exitStatus: 127
      # - containerStopped:
      #     actionId: 4
      #     exitStatus: 127
      #     stderr: |
      #       bash: line 3: /mnt/data/script/foo: No such file or directory
      #   description: 'Stopped running "user-command": exit status 127: ...
      #   timestamp: '2020-09-28T23:10:04.671133099Z'

      # If we can get a containerStopped event, it has the best information
      # Otherwise fallback to unexpectedExitStatus.
      # Otherwise fallback to failed.

      container_failed_events = google_v2_operations.get_container_stopped_error_events(
          self._op)
      unexpected_exit_events = google_v2_operations.get_unexpected_exit_events(
          self._op)
      failed_events = google_v2_operations.get_failed_events(self._op)

      if container_failed_events:
        container_failed_event = container_failed_events[-1]
        action_id = google_v2_operations.get_event_action_id(
            container_failed_event)
        msg = google_v2_operations.get_event_description(container_failed_event)
        detail = google_v2_operations.get_event_stderr(container_failed_event)

      elif unexpected_exit_events:
        unexpected_exit_event = unexpected_exit_events[-1]
        action_id = google_v2_operations.get_event_action_id(
            unexpected_exit_event)
        msg = google_v2_operations.get_event_description(unexpected_exit_event)

      elif failed_events:
        failed_event = failed_events[-1]
        msg = google_v2_operations.get_event_description(failed_event)
        action_id = None

      if not msg:
        error = google_v2_operations.get_error(self._op)
        if error:
          msg = error['message']

        action = google_v2_operations.get_action_by_id(self._op, action_id)

    return msg, action, detail

  def _is_ssh_enabled(self, op):
    """Return whether the operation had --ssh enabled or not."""
    action = google_v2_operations.get_action_by_name(op, 'ssh')
    return action is not None

  def error_message(self):
    """Returns an error message if the operation failed for any reason.

    Failure as defined here means ended for any reason other than 'success'.
    This means that a successful cancelation will also return an error message.

    Returns:
      string, string will be empty if job did not error.
    """
    error = google_v2_operations.get_error(self._op)
    if error:
      job_id = self.get_field('job-id')
      task_id = self.get_field('task-id')
      task_str = job_id if task_id is None else '{} (task: {})'.format(
          job_id, task_id)

      return 'Error in {} - code {}: {}'.format(task_str, error['code'],
                                                error['message'])

    return ''

  def get_field(self, field, default=None):
    """Returns a value from the operation for a specific set of field names.

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
      value = self._op['name']
    elif field == 'user-project':
      if self._job_descriptor:
        value = self._job_descriptor.job_metadata.get(field)
    elif field in [
        'job-id', 'job-name', 'task-id', 'task-attempt', 'user-id',
        'dsub-version'
    ]:
      value = google_v2_operations.get_label(self._op, field)
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
            self._job_descriptor.task_descriptors[0].task_params, field)
        value = {item.name: item.value for item in items}
    elif field in [
        'inputs', 'outputs', 'input-recursives', 'output-recursives'
    ]:
      if self._job_descriptor:
        value = {}
        items = providers_util.get_job_and_task_param(
            self._job_descriptor.job_params,
            self._job_descriptor.task_descriptors[0].task_params, field)
        value.update({item.name: item.value for item in items})
    elif field == 'mounts':
      if self._job_descriptor:
        items = providers_util.get_job_and_task_param(
            self._job_descriptor.job_params,
            self._job_descriptor.task_descriptors[0].task_params, field)
        value = {item.name: item.value for item in items}
    elif field == 'create-time':
      ds = google_v2_operations.get_create_time(self._op)
      value = google_base.parse_rfc3339_utc_string(ds)
    elif field == 'start-time':
      ds = google_v2_operations.get_start_time(self._op)
      if ds:
        value = google_base.parse_rfc3339_utc_string(ds)
    elif field == 'end-time':
      ds = google_v2_operations.get_end_time(self._op)
      if ds:
        value = google_base.parse_rfc3339_utc_string(ds)

    elif field == 'status':
      # Short message like:
      #   "Pending", "VM starting", "<error message>", "Success", "Cancelled"
      value = self._operation_status()

    elif field == 'status-message':
      # Longer message
      msg, action, detail = self._operation_status_message()
      if msg.startswith('Execution failed:'):
        # msg may look something like
        # "Execution failed: action 2: pulling image..."
        # Emit the actual message ("pulling image...")
        msg = msg.split(': ', 2)[-1]
      value = msg

    elif field == 'status-detail':
      # As much detail as we can reasonably get from the operation
      msg, action, detail = self._operation_status_message()
      if detail:
        msg = detail

      if action:
        value = google_v2_operations.get_action_name(action) + ':\n' + msg
      else:
        value = msg

    elif field == 'last-update':
      last_update = google_v2_operations.get_last_update(self._op)
      if last_update:
        value = google_base.parse_rfc3339_utc_string(last_update)
    elif field == 'provider':
      return self._provider_name
    elif field == 'provider-attributes':
      value = {}

      # The ssh flag is determined by if an action named 'ssh' exists.
      value['ssh'] = self._is_ssh_enabled(self._op)

      value[
          'block-external-network'] = google_v2_operations.external_network_blocked(
              self._op)

      # The VM instance name and zone can be found in the WorkerAssignedEvent.
      # For a given operation, this may have occurred multiple times, so be
      # sure to grab the most recent.
      assigned_event_details = google_v2_operations.get_worker_assigned_event_details(
          self._op)
      if assigned_event_details:
        value['instance-name'] = assigned_event_details.get('instance')
        value['zone'] = assigned_event_details.get('zone')

      # The rest of the information comes from the request itself.
      # Note that for the v2alpha1 API, the returned operation contains
      # default values in the response, while the v2beta API omits fields
      # that match empty defaults (hence the "False", "[]", and empty string
      # default values in the get() calls below).
      resources = google_v2_operations.get_resources(self._op)
      value['regions'] = resources.get('regions', [])
      value['zones'] = resources.get('zones', [])
      if 'virtualMachine' in resources:
        vm = resources['virtualMachine']
        value['machine-type'] = vm.get('machineType')
        value['preemptible'] = vm.get('preemptible', False)

        value['boot-disk-size'] = vm.get('bootDiskSizeGb')
        value['network'] = google_v2_operations.get_vm_network_name(vm) or ''
        value['subnetwork'] = vm.get('network', {}).get('subnetwork', '')
        value['use_private_address'] = vm.get('network',
                                              {}).get('usePrivateAddress',
                                                      False)
        value['cpu_platform'] = vm.get('cpuPlatform', '')
        value['accelerators'] = vm.get('accelerators', [])
        value['enable-stackdriver-monitoring'] = vm.get(
            'enableStackdriverMonitoring', False)
        value['service-account'] = vm.get('serviceAccount', {}).get('email')

        # dsub now use "volumes" instead of "disks" (following the lead of the
        # Life Sciences API). This block is included for compatibility with
        # jobs in the operations list run by older versions of dsub.
        if vm.get('disks'):
          datadisk = next((d for d in vm['disks']
                           if d['name'] == google_utils.DATA_DISK_NAME))
          if datadisk:
            value['disk-size'] = datadisk.get('sizeGb')
            value['disk-type'] = datadisk.get('type')
        if vm.get('volumes'):
          volumes = []
          for v in vm['volumes']:
            if v['volume'] == google_utils.DATA_DISK_NAME:
              d = v.get('persistentDisk', {})
              value['disk-size'] = d.get('sizeGb')
              value['disk-type'] = d.get('type')
            else:
              d = None
              if v.get('persistentDisk'):
                d = v.get('persistentDisk')
              elif v.get('existingDisk'):
                d = v.get('existingDisk')

              if d:
                volume = {'name': v['volume']}
                if d.get('disk'):
                  volume['disk-name'] = d.get('disk')
                if d.get('type'):
                  volume['disk-type'] = d.get('type')
                if d.get('sizeGb'):
                  volume['disk-size'] = d.get('sizeGb')
                volumes.append(volume)

            value['volumes'] = volumes

    elif field == 'events':
      value = GoogleV2EventMap(self._op).get_filtered_normalized_events()
    elif field == 'script-name':
      if self._job_descriptor:
        value = self._job_descriptor.job_metadata.get(field)
    elif field == 'script':
      value = self._try_op_to_script_body()
    else:
      raise ValueError('Unsupported field: "%s"' % field)

    return value if value else default


if __name__ == '__main__':
  pass
