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

This module implements job creation, listing, and canceling using the
Google Genomics Pipelines and Operations APIs v2alpha1.

Status: *early* development
Much still to be done just on launching a pipeline, including localization,
delocalization, logging.
Need to figure out support for generic scripts (currently only supports bash).
Then dstat/ddel support.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import os
import textwrap

from . import base
from . import google_base
from . import google_v2_operations
from . import google_v2_pipelines

from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util

_PROVIDER_NAME = 'google-v2'

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS

# Action steps that interact with GCS need gsutil.
# Use the 'alpine' variant of the cloud-sdk Docker image as it is much smaller.
_CLOUD_SDK_IMAGE = 'google/cloud-sdk:alpine'

# The prepare step needs Python.
# Use the 'slim' variant of the python Docker image as it is much smaller.
_PYTHON_IMAGE = 'python:2.7-slim'

# Name of the data disk
_DATA_DISK_NAME = 'datadisk'

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
_LOGGING_CMD = textwrap.dedent("""\
  gsutil cp /google/logs/action/{user_container}/stdout {stdout_path} &
  gsutil cp /google/logs/action/{user_container}/stderr {stderr_path} &
  gsutil cp /google/logs/output {logging_path} &
  wait
""")

_CONTINUOUS_LOGGING_CMD = textwrap.dedent("""\
  while :; do
    {logging_cmd}

    if [[ -d /google/logs/action/{last_action} ]]; then
      break
    fi
    sleep 10s
  done
""")

# Command to create the directories for the dsub user environment
_MK_RUNTIME_DIRS_CMD = '\n'.join('mkdir -m 777 -p "%s" ' % dir for dir in [
    providers_util.SCRIPT_DIR, providers_util.TMP_DIR,
    providers_util.WORKING_DIR
])

# The user's script or command is made available to the container in
#   /mnt/data/script/<script-name>
#
# To get it there, it is passed in through the environment in the "prepare"
# action and "echo"-ed to a file.
#
# Pipelines v2alpha1 uses Docker environment files which do not support
# multi-line environment variables, so we encode the script using Python's
# repr() function and then decoded it using ast.literal_eval().
# This has the advantage over other encoding schemes (such as base64) of being
# user-readable in the Genomics "operation" object.
_SCRIPT_VARNAME = '_SCRIPT_REPR'

_PYTHON_DECODE_SCRIPT = textwrap.dedent("""\
  import ast
  import sys

  sys.stdout.write(ast.literal_eval(sys.stdin.read()))
""")

_PREPARE_CMD = textwrap.dedent("""\
  #!/bin/bash

  set -o errexit
  set -o nounset

  {mk_runtime_dirs}

  echo "${{{script_var}}}" \
    | python -c '{python_decode_script}' \
    > {script_path}
  chmod u+x {script_path}
""")


class GoogleV2JobProvider(base.JobProvider):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(self, verbose, dry_run, project, credentials=None):
    self._verbose = verbose
    self._dry_run = dry_run

    self._project = project

    self._service = google_base.setup_service('genomics', 'v2alpha1',
                                              credentials)

  def prepare_job_metadata(self, script, job_name, user_id, create_time):
    """Returns a dictionary of metadata fields for the job."""
    return google_base.prepare_job_metadata(script, job_name, user_id,
                                            create_time)

  def _get_logging_command(self, user_container, logging_uri):
    """Returns the base command for copying logging files."""
    if not logging_uri.endswith('.log'):
      raise ValueError('Logging URI must end in ".log": {}'.format(logging_uri))

    logging_prefix = logging_uri[:-len('.log')]
    return _LOGGING_CMD.format(
        user_container=user_container,
        logging_path='{}.log'.format(logging_prefix),
        stdout_path='{}-stdout.log'.format(logging_prefix),
        stderr_path='{}-stderr.log'.format(logging_prefix))

  def _build_user_environment(self, envs, inputs, outputs):
    """Returns a dictionary of for the user container environment."""
    envs = {env.name: env.value for env in envs}
    envs.update(providers_util.get_file_environment_variables(inputs))
    envs.update(providers_util.get_file_environment_variables(outputs))

    return envs

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
        disk=_DATA_DISK_NAME,
        path=providers_util.DATA_MOUNT_POINT,
        read_only=False)
    scopes = job_resources.scopes or google_base.DEFAULT_SCOPES

    # Set up the task labels
    labels = {
        label.name: label.value if label.value else '' for label in
        google_base.build_pipeline_labels(job_metadata, task_metadata)
        | job_params['labels'] | task_params['labels']
    }

    # Set up the logging command
    logging_cmd = self._get_logging_command(3, task_resources.logging_path.uri)

    # Set up user script and environment
    script = task_view.job_metadata['script']

    envs = job_params['envs'] | task_params['envs']
    inputs = job_params['inputs'] | task_params['inputs']
    outputs = job_params['outputs'] | task_params['outputs']
    user_environment = self._build_user_environment(envs, inputs, outputs)

    script_path = os.path.join(providers_util.SCRIPT_DIR, script.name)
    prepare_command = _PREPARE_CMD.format(
        mk_runtime_dirs=_MK_RUNTIME_DIRS_CMD,
        script_var=_SCRIPT_VARNAME,
        python_decode_script=_PYTHON_DECODE_SCRIPT,
        script_path=script_path)

    # The list of "actions" will be:
    #   1- continuous copy of log files off to Cloud Storage
    #   2- localize objects from Cloud Storage to block storage
    #   3- execute user command
    #   4- delocalize objects from block storage to Cloud Storage
    #   5- final copy of log files off to Cloud Storage

    actions = [
        google_v2_pipelines.build_action(
            name='continuous_logging',
            flags='RUN_IN_BACKGROUND',
            image_uri=_CLOUD_SDK_IMAGE,
            entrypoint='/bin/bash',
            commands=[
                '-c',
                _CONTINUOUS_LOGGING_CMD.format(
                    logging_cmd=logging_cmd, last_action=4)
            ]),
        google_v2_pipelines.build_action(
            name='prepare',
            image_uri=_PYTHON_IMAGE,
            mounts=[mnt_datadisk],
            environment={_SCRIPT_VARNAME: repr(script.value)},
            entrypoint='/bin/bash',
            commands=['-c', prepare_command]),
        google_v2_pipelines.build_action(
            name=script.name,
            image_uri=job_resources.image,
            mounts=[mnt_datadisk],
            environment=user_environment,
            entrypoint='/bin/bash',
            commands=['-c', script_path]),
        google_v2_pipelines.build_action(
            name='final_logging',
            flags='ALWAYS_RUN',
            image_uri=_CLOUD_SDK_IMAGE,
            entrypoint='/bin/bash',
            commands=['-c', logging_cmd]),
    ]

    disks = [
        google_v2_pipelines.build_disks(_DATA_DISK_NAME,
                                        job_resources.disk_size)
    ]
    network = google_v2_pipelines.build_network(None, None)
    machine_type = job_resources.machine_type or job_model.DEFAULT_MACHINE_TYPE
    service_account = google_v2_pipelines.build_service_account(
        'default', scopes)

    resources = google_v2_pipelines.build_resources(
        self._project,
        job_resources.regions,
        google_base.get_zones(job_resources.zones),
        google_v2_pipelines.build_machine(
            network=network,
            machine_type=machine_type,
            preemptible=job_resources.preemptible,
            service_account=service_account,
            boot_disk_size_gb=job_resources.boot_disk_size,
            disks=disks,
            labels=labels),
    )

    pipeline = google_v2_pipelines.build_pipeline(actions, resources, None)

    return {'pipeline': pipeline, 'labels': labels}

  def _submit_pipeline(self, request):
    operation = google_base.Api.execute(
        self._service.pipelines().run(body=request))
    if self._verbose:
      print('Launched operation {}'.format(operation['name']))

    return GoogleOperation(operation).get_field('task-id')

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
        provider_name=_PROVIDER_NAME,
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
        if dsub_util.outputs_are_present(outputs):
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
      print(json.dumps(requests, indent=2, sort_keys=True))

    if not requests and not launched_tasks:
      return {'job-id': dsub_util.NO_JOB}

    return {
        'job-id': job_descriptor.job_metadata['job-id'],
        'user-id': job_descriptor.job_metadata['user-id'],
        'task-id': [task_id for task_id in launched_tasks if task_id],
    }

  def get_tasks_completion_messages(self, tasks):
    pass

  def _get_status_filters(self, statuses):
    if not statuses or statuses == {'*'}:
      return None

    return [google_v2_operations.STATUS_FILTER_MAP[s] for s in statuses]

  def _get_label_filters(self, label_key, values):
    if not values or values == {'*'}:
      return None

    return [google_v2_operations.label_filter(label_key, v) for v in values]

  def _get_labels_filters(self, labels):
    if not labels:
      return None

    return [google_v2_operations.label_filter(l.name, l.value) for l in labels]

  def _get_create_time_filters(self, create_time_min, create_time_max):
    filters = []
    for create_time, comparator in [(create_time_min, '>='), (create_time_max,
                                                              '<=')]:
      if not create_time:
        continue

      filters.append(
          google_v2_operations.create_time_filter(create_time, comparator))
    return filters

  def _build_query_filter(self,
                          statuses,
                          user_ids=None,
                          job_ids=None,
                          job_names=None,
                          task_ids=None,
                          task_attempts=None,
                          labels=None,
                          create_time_min=None,
                          create_time_max=None):
    # The Pipelines v2 API allows for building fairly elaborate filter
    # clauses. We can group (). We can AND, OR, and NOT.
    #
    # The first set of filters, labeled here as OR filters are elements
    # where more than one value cannot be true at the same time. For example,
    # an operation cannot have a status of both RUNNING and CANCELED.
    #
    # The second set of filters, labeled here as AND filters are elements
    # where more than one value can be true. For example,
    # an operation can have a label with key1=value2 AND key2=value2.

    # Translate the semantic requests into a v2alpha1-specific filter.

    # 'OR' filtering arguments.
    status_filters = self._get_status_filters(statuses)
    user_id_filters = self._get_label_filters('user-id', user_ids)
    job_id_filters = self._get_label_filters('job-id', job_ids)
    job_name_filters = self._get_label_filters('job-name', job_names)
    task_id_filters = self._get_label_filters('task-id', task_ids)
    task_attempt_filters = self._get_label_filters('task-attempt',
                                                   task_attempts)
    # 'AND' filtering arguments.
    label_filters = self._get_labels_filters(labels)
    create_time_filters = self._get_create_time_filters(create_time_min,
                                                        create_time_max)

    if job_id_filters and job_name_filters:
      raise ValueError(
          'Filtering by both job IDs and job names is not supported')

    # Now build up the full text filter.
    # OR all of the OR filter arguments together.
    # AND all of the AND arguments together.
    or_arguments = []
    for or_filters in [
        status_filters, user_id_filters, job_id_filters, job_name_filters,
        task_id_filters, task_attempt_filters
    ]:
      if or_filters:
        or_arguments.append('(' + ' OR '.join(or_filters) + ')')

    and_arguments = []
    for and_filters in [label_filters, create_time_filters]:
      if and_filters:
        and_arguments.append('(' + ' AND '.join(and_filters) + ')')

    # Now and all of these arguments together.
    return ' AND '.join(or_arguments + and_arguments)

  def _operations_list(self, ops_filter, page_size=0):
    """Gets the list of operations for the specified filter.

    Args:
      ops_filter: string filter of operations to return
      page_size: the number of operations to requested on each list operation to
        the pipelines API (if 0 or None, the API default is used)

    Yields:
      Operations matching the filter criteria.
    """

    page_token = None
    more_operations = True
    documented_default_page_size = 256
    documented_max_page_size = 2048

    if not page_size:
      page_size = documented_default_page_size
    page_size = min(page_size, documented_max_page_size)

    while more_operations:
      api = self._service.projects().operations().list(
          name='projects/{}/operations'.format(self._project),
          filter=ops_filter,
          pageToken=page_token,
          pageSize=page_size)
      response = google_base.Api.execute(api)

      ops = response.get('operations', [])
      for op in ops:
        if google_v2_operations.is_dsub_operation(op):
          yield GoogleOperation(op)

      page_token = response.get('nextPageToken')
      more_operations = bool(page_token)

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
      user_ids: a list of ids for the user(s) who launched the job.
      job_ids: a list of job ids to return.
      job_names: a list of job names to return.
      task_ids: a list of specific tasks within the specified job(s) to return.
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

    Yeilds:
      Genomics API Operations objects.
    """

    ops_filter = self._build_query_filter(
        statuses, user_ids, job_ids, job_names, task_ids, task_attempts, labels,
        create_time_min, create_time_max)

    # The pipelines API returns operations sorted by create-time date. We can
    # use this sorting guarantee to merge-sort the streams together and only
    # retrieve more tasks as needed.
    stream = self._operations_list(ops_filter, page_size=page_size)

    tasks_yielded = 0
    for task in stream:
      yield task
      tasks_yielded += 1
      if 0 < max_tasks < tasks_yielded:
        break

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    pass


class GoogleOperation(base.Task):
  """Task wrapper around a Pipelines API operation object."""

  def __init__(self, operation_data):
    self._op = operation_data

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
    """Returns the most relevant status string and last updated date string.

    This string is meant for display only.

    Returns:
      A printable status string and date string.
    """
    if not google_v2_operations.is_done(self._op):
      last_event = google_v2_operations.get_last_event(self._op)
      if last_event:
        msg = last_event['description']
      else:
        msg = 'Pending'
    else:
      error = google_v2_operations.get_error(self._op)
      if error:
        msg = error['message']
      else:
        msg = 'Success'

    return msg

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
    elif field in ['job-id', 'job-name', 'task-id', 'user-id', 'dsub-version']:
      value = google_v2_operations.get_label(self._op, field)
    elif field == 'task-status':
      value = self._operation_status()
    elif field == 'logging':
      value = 'TODO'
    elif field == 'envs':
      value = 'TODO'
    elif field == 'labels':
      # Reserved labels are filtered from dsub task output.
      value = {
          k: v
          for k, v in google_v2_operations.get_labels(self._op).items()
          if k not in job_model.RESERVED_LABELS
      }
    elif field == 'inputs':
      value = 'TODO'
    elif field == 'outputs':
      value = 'TODO'
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
      value = self._operation_status()
    elif field in ['status-message', 'status-detail']:
      status = self._operation_status_message()
      value = status
    elif field == 'last-update':
      last_update = google_v2_operations.get_last_update(self._op)
      if last_update:
        value = google_base.parse_rfc3339_utc_string(last_update)
    elif field == 'provider':
      return _PROVIDER_NAME
    elif field == 'provider-attributes':
      value = 'TODO'
    else:
      raise ValueError('Unsupported field: "%s"' % field)

    return value if value else default


if __name__ == '__main__':
  pass
