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

# Special dsub directories within the Docker container
_SCRIPT_DIR = '%s/script' % providers_util.DATA_MOUNT_POINT
_TMP_DIR = '%s/tmp' % providers_util.DATA_MOUNT_POINT
_WORKING_DIR = '%s/workingdir' % providers_util.DATA_MOUNT_POINT

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
_MK_RUNTIME_DIRS_CMD = '\n'.join(
    'mkdir -m 777 -p "%s" ' % dir
    for dir in [_SCRIPT_DIR, _TMP_DIR, _WORKING_DIR])

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

    script_path = os.path.join(_SCRIPT_DIR, script.name)
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

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    pass

  def get_tasks_completion_messages(self, tasks):
    pass

  def prepare_job_metadata(self, script, job_name, user_id, create_time):
    """Returns a dictionary of metadata fields for the job."""
    return google_base.prepare_job_metadata(script, job_name, user_id,
                                            create_time)

  def lookup_job_tasks(self,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0):
    pass


class GoogleOperation(base.Task):
  """Task wrapper around a Pipelines API operation object."""

  def __init__(self, operation_data):
    self._op = operation_data

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

    return 'not implemented'


if __name__ == '__main__':
  pass
