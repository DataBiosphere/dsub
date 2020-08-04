# Lint as: python2, python3
# Copyright 2016 Google Inc. All Rights Reserved.
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
"""Stub provider, for unit testing. Does not actually run anything."""

from . import base


class StubJobProvider(base.JobProvider):
  """Stub provider, for unit testing. Does not actually run anything."""

  def __init__(self):
    self._operations = []

  # 1) Methods that are supposed to do something. Use mocks
  #    if you need to check that they are called.

  def submit_job(self, job_descriptor, skip_if_output_present):
    pass

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  task_attempts,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    pass

  # 2) Methods that manipulate the state of the fictional operations.
  #    Meant to be called by the test code, to set things up.

  def set_operations(self, ops):
    """Set the state of the fictional world.

    Args:
     ops: a list of dict, each representing an operation.

    Operations can have the following fields:
       - status: tuple (string,date)
       - user: string
       - job-id: string
       - job-name: string
       - task-id: string
       - task-attempt: integer
       - labels: list<dict>
       - status-message: string
       - error-messages : list of string
    """
    self._operations = [StubTask(o) for o in ops]

  def get_operations(self):
    return self._operations

  # 3) Methods that return information.
  #    Meant to be called by the code under test, they rely on the fake
  #    state set via group (2) above.

  def prepare_job_metadata(self, script, job_name, user_id):
    del script, job_name, user_id  # pacify linter
    raise BaseException('Not implemented')

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
                       verbose=True):
    """Return a list of operations. See base.py for additional detail."""
    statuses = None if statuses == {'*'} else statuses
    user_ids = None if user_ids == {'*'} else user_ids
    job_ids = None if job_ids == {'*'} else job_ids
    job_names = None if job_names == {'*'} else job_names
    task_ids = None if task_ids == {'*'} else task_ids
    task_attempts = None if task_attempts == {'*'} else task_attempts

    if labels or create_time_min or create_time_max:
      raise NotImplementedError(
          'Lookup by labels and create_time not yet supported by stub.')

    operations = [
        x for x in self._operations
        if ((not statuses or x.get_field('status', (None, None))[0] in statuses
            ) and (not user_ids or x.get_field('user', None) in user_ids) and
            (not job_ids or x.get_field('job-id', None) in job_ids) and
            (not job_names or x.get_field('job-name', None) in job_names) and
            (not task_ids or x.get_field('task-id', None) in task_ids) and
            (not task_attempts or
             x.get_field('task-attempt', None) in task_attempts))
    ]
    if max_tasks > 0:
      operations = operations[:max_tasks]
    return operations

  def get_tasks_completion_messages(self, tasks):
    error_messages = []
    for task in tasks:
      error_messages += [task.get_field('error-message', '')]

    return error_messages


class StubTask(base.Task):

  def __init__(self, op):
    self.op = op

  def get_field(self, field, default=None):
    if field == 'task-status':
      return self.op['status'][0]
    elif field == 'task-id':
      return self.op['task-id']
    elif field == 'provider-attributes':
      preempted = self.op.get('error-message') == 'preempted'
      return {'preempted': preempted}
    elif field == 'status-message':
      return self.op['status-message']
    elif field == 'error-message':
      return self.op.get('error-message')
    return self.op.get(field, None)

  def raw_task_data(self):
    return self.op

if __name__ == '__main__':
  pass
