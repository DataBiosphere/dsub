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

from . import base


class FailsException(Exception):
  pass


class FailsJobProvider(base.JobProvider):
  """Provider, for e2e testing. Always fails."""

  def __init__(self):
    self._operations = []

  def submit_job(self, job_resources, job_metadata, all_job_data):
    del job_resources, job_metadata, all_job_data  # we fail unconditionally
    raise FailsException("fails provider made submit_job fail")

  def delete_jobs(self, user_list, job_list, task_list, create_time=None):
    del user_list, job_list, task_list, create_time  # we fail unconditionally
    raise FailsException("fails provider made delete_jobs fail")

  def get_task_field(self, job, field, default=None):
    del job, field  # unused
    return default

  def lookup_job_tasks(self,
                       status_list,
                       user_list=None,
                       job_list=None,
                       task_list=None,
                       max_tasks=0):
    del status_list, user_list, job_list, task_list, max_tasks  # never any jobs
    raise FailsException("fails provider made lookup_job_tasks fail")

  def get_task_status_message(self, task):
    del task  # doesn't matter
    return "DOOMED"

  def get_tasks_completion_messages(self, tasks):
    del tasks  # doesn't matter either
    return ["Fail provider never completes a job"]

  def prepare_job_metadata(self, script, job_name, user_id):
    del script, job_name, user_id  # all the same
    return {"job-id": "DOOMED_JOB"}


if __name__ == "__main__":
  pass
