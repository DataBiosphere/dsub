# Lint as: python2, python3
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
"""Stub provider, for testing.

* Raises an exception for job submission, listing, or deletion.
* Returns uninteresting values for other public provider methods.
"""
from __future__ import print_function

from . import base
from ..lib import dsub_util
from ..lib import job_model


class FailsException(Exception):
  pass


class FailsJobProvider(base.JobProvider):
  """Provider, for e2e testing. Always fails."""

  def __init__(self):
    self._operations = []

  def submit_job(self, job_descriptor, skip_if_output_present):
    """Fails if there's anything to submit (so, not skipped)."""
    if not skip_if_output_present:
      raise FailsException("fails provider made submit_job fail")

    for task_view in job_model.task_view_generator(job_descriptor):
      job_params = task_view.job_params
      task_params = task_view.task_descriptors[0].task_params

      outputs = job_params["outputs"] | task_params["outputs"]
      if dsub_util.outputs_are_present(outputs):
        print("Skipping task because its outputs are present")
        continue

      # if any task is allowed to run, then we fail.
      raise FailsException("fails provider made submit_job fail")

    return {"job-id": dsub_util.NO_JOB}

  def delete_jobs(self, user_ids, job_ids, task_ids, labels, create_time=None):
    del user_ids, job_ids, task_ids, labels, create_time
    raise FailsException("fails provider made delete_jobs fail")

  def lookup_job_tasks(self,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       labels=None,
                       create_time=None,
                       max_tasks=0):
    del statuses, user_ids, job_ids, job_names, task_ids, labels, max_tasks
    raise FailsException("fails provider made lookup_job_tasks fail")

  def get_tasks_completion_messages(self, tasks):
    del tasks  # doesn't matter either
    return ["Fail provider never completes a job"]

  def prepare_job_metadata(self, script, job_name, user_id):
    del script, job_name, user_id  # all the same
    return {
        "job-name": "DOOMED_JOB_NAME",
        "user-id": "DOOMED_JOB_USER",
    }


class FailTask(base.Task):
  """Dummy task."""

  def raw_task_data(self):
    return {}

  def get_field(self, field, default=None):
    del field
    return default
if __name__ == "__main__":
  pass
