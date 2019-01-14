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
"""Unit tests for the local provider.
"""

from __future__ import absolute_import
from __future__ import print_function

import copy
import datetime
import unittest
from dsub.lib import dsub_util
from dsub.lib import job_model
from dsub.providers import local
import pytz

# The local provider can launch tasks quickly enough that they end up with the
# same timestamp. The goal of this test below is to verify that tasks come back
# sorted in a way expected by users: "most recent first".
#
# Also - originally the local provider did not serialize out the task create
# time (just the job create time), so we need to be sure the sort routine does
# not blow up if one is not provided.


# Define a utility function for creating a task object with minimal elements
# required for the sort test.
def _make_task(job_create_time, task_create_time, task_id):
  return local.LocalTask(
      job_descriptor=job_model.JobDescriptor(
          job_metadata={'create-time': job_create_time},
          job_params=None,
          job_resources=None,
          task_descriptors=[
              job_model.TaskDescriptor(
                  task_metadata={
                      'task-id': task_id,
                      'create-time': task_create_time
                  },
                  task_params=None,
                  task_resources=None)
          ]),
      task_status=None,
      log_detail=None,
      end_time=None,
      last_update=None,
      pid=None,
      events=None,
      script=None)


CREATE_TIME_1 = dsub_util.replace_timezone(
    datetime.datetime(2018, 1, 1, 22, 28, 37, 321788), pytz.utc)

CREATE_TIME_2 = dsub_util.replace_timezone(
    datetime.datetime(2018, 1, 2, 22, 28, 37, 321788), pytz.utc)

CREATE_TIME_3 = dsub_util.replace_timezone(
    datetime.datetime(2018, 1, 3, 22, 28, 37, 321788), pytz.utc)

# Simple list of tasks that are in exactly the wrong order:
#  Job 1: two tasks (both with timestamp 1)
#  Job 2: two tasks  (both with timestamp 2)
#  Job 3: one implicit task (with timestamp 3)
# To ensure that the sort picks up the task create time, but falls back to the
# job create time, set the job create time the same for all of them.
REVERSE_ORDERED_TASKS = [
    _make_task(CREATE_TIME_1, None, 1),
    _make_task(CREATE_TIME_1, None, 2),
    _make_task(CREATE_TIME_1, CREATE_TIME_2, 1),
    _make_task(CREATE_TIME_1, CREATE_TIME_2, 2),
    _make_task(CREATE_TIME_1, CREATE_TIME_3, None),
]


class TestSortTasks(unittest.TestCase):

  def test_sort_tasks(self):
    tasks = copy.deepcopy(REVERSE_ORDERED_TASKS)
    local._sort_tasks(tasks)

    self.assertEqual(len(tasks), len(REVERSE_ORDERED_TASKS))
    self.assertEqual(tasks[0].get_field('create-time'), CREATE_TIME_3)
    self.assertEqual(tasks[0].get_field('task-id'), None)
    self.assertEqual(tasks[1].get_field('create-time'), CREATE_TIME_2)
    self.assertEqual(tasks[1].get_field('task-id'), 2)
    self.assertEqual(tasks[2].get_field('create-time'), CREATE_TIME_2)
    self.assertEqual(tasks[2].get_field('task-id'), 1)
    self.assertEqual(tasks[3].get_field('create-time'), CREATE_TIME_1)
    self.assertEqual(tasks[3].get_field('task-id'), 2)
    self.assertEqual(tasks[4].get_field('create-time'), CREATE_TIME_1)
    self.assertEqual(tasks[4].get_field('task-id'), 1)


if __name__ == '__main__':
  unittest.main()
