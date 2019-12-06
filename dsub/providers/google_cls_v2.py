# Lint as: python2, python3
# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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
Google Cloud Life Sciences Pipelines and Operations APIs v2beta.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from . import base
from . import google_base

_PROVIDER_NAME = 'google-cls-v2'


class GoogleCLSV2JobProvider(base.JobProvider):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(self, dry_run, project, credentials=None):
    self._dry_run = dry_run

    self._project = project

    self._service = google_base.setup_service('lifesciences', 'v2beta',
                                              credentials)

  def submit_job(self, job_descriptor):
    raise NotImplementedError()

  def get_tasks_completion_messages(self, tasks):
    """List of the error message of each given task."""
    raise NotImplementedError()

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    raise NotImplementedError()

  def prepare_job_metadata(self, script, job_name, user_id, create_time):
    raise NotImplementedError()

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
    raise NotImplementedError()


if __name__ == '__main__':
  pass
