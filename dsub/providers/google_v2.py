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

from . import base
from . import google_base
from . import google_v2_pipelines

from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util

_PROVIDER_NAME = 'google-v2'

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS


class GoogleV2JobProvider(base.JobProvider):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(self, verbose, dry_run, project, credentials=None):
    self._verbose = verbose
    self._dry_run = dry_run

    self._project = project

    self._service = google_base.setup_service('genomics', 'v2alpha1',
                                              credentials)

  def _build_pipeline_request(self, task_view):
    """Returns a Pipeline objects for the job."""
    job_metadata = task_view.job_metadata
    job_params = task_view.job_params
    job_resources = task_view.job_resources
    task_metadata = task_view.task_descriptors[0].task_metadata
    task_params = task_view.task_descriptors[0].task_params

    script = task_view.job_metadata['script']

    labels = {
        label.name: label.value if label.value else '' for label in
        google_base.build_pipeline_labels(job_metadata, task_metadata)
        | job_params['labels'] | task_params['labels']
    }

    # The list of "actions" is:
    #   1- localize objects from Cloud Storage to block storage
    #   2- execute user command
    #   3- delocalize objects from block storage to Cloud Storage

    # For now, the actions just execute a user command as POC.
    actions = [
        google_v2_pipelines.build_action(
            name=script.name,
            image_uri=job_resources.image,
            entrypoint='/bin/bash',
            commands=['-c', script.value]),
    ]

    disks = None
    network = google_v2_pipelines.build_network(None, None)
    machine_type = job_resources.machine_type or job_model.DEFAULT_MACHINE_TYPE
    service_account = google_v2_pipelines.build_service_account(
        'default', job_resources.scopes or job_model.DEFAULT_SCOPES)

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
