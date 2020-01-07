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
"""Unit tests for the _wait_and_retry loop."""

import unittest
from dsub.commands import dsub as dsub_command
from dsub.lib import job_model
from dsub.lib import param_util
from dsub.providers import stub
import fake_time
import parameterized

PREEMPTED_TASK = {
    'job-id': 'job-1',
    'status': ('FAILURE', 123),
    'task-id': '123',
    'status-message': 'preempted',
    'error-message': 'preempted'
}

FAILED_TASK = {
    'job-id': 'job-1',
    'status': ('FAILURE', 123),
    'task-id': '123',
    'status-message': 'non-preemption failure',
    'error-message': 'non-preemption failure'
}

SUCCESSFUL_TASK = {
    'job-id': 'job-1',
    'status': ('SUCCESS', 123),
    'task-id': '123',
    'status-message': 'Success!'
}


def establish_chronology(chronology):
  dsub_command.SLEEP_FUNCTION = fake_time.FakeTime(chronology).sleep


class TestWaitAndRetry(unittest.TestCase):

  def setUp(self):
    super(TestWaitAndRetry, self).setUp()
    self.provider = stub.StubJobProvider()

  @parameterized.parameterized.expand([
      # P == R, eventual success
      (1, 1, [[PREEMPTED_TASK], [SUCCESSFUL_TASK]], []),
      # P == R, all preempted
      (1, 1, [[PREEMPTED_TASK], [PREEMPTED_TASK,
                                 PREEMPTED_TASK]], [['preempted']]),
      # P == R, non-preemption failure
      (1, 1, [[FAILED_TASK], [FAILED_TASK,
                              FAILED_TASK]], [['non-preemption failure']]),
      # R > P, eventual success
      (2, 1, [[PREEMPTED_TASK], [PREEMPTED_TASK, PREEMPTED_TASK],
              [SUCCESSFUL_TASK]], []),
      # R > P, failure
      (2, 1, [[PREEMPTED_TASK], [FAILED_TASK, FAILED_TASK],
              [FAILED_TASK, FAILED_TASK,
               FAILED_TASK]], [['non-preemption failure']]),
      # no preemptible flag, R given, success
      (1, 0, [[SUCCESSFUL_TASK]], []),
      # no preemptible flag, R given, non-preemption failure
      (1, 0, [[FAILED_TASK], [FAILED_TASK,
                              FAILED_TASK]], [['non-preemption failure']]),
      # --preemptible, R given, eventual success
      (1, True, [[PREEMPTED_TASK], [SUCCESSFUL_TASK]], [])
  ])
  def test_foo(self, retries, max_preemptible_attempts,
               list_of_list_of_operations, expected):

    def chronology(list_of_list_of_operations):
      for operations in list_of_list_of_operations:
        self.provider.set_operations(operations)
        yield 1

    establish_chronology(chronology(list_of_list_of_operations))

    job_descriptor = job_model.JobDescriptor(
        job_metadata={'create-time': 123456},
        job_params=None,
        job_resources=job_model.Resources(
            logging=job_model.LoggingParam('gs://buck/logs', job_model.P_GCS),
            max_preemptible_attempts=param_util.PreemptibleParam(
                max_preemptible_attempts)),
        task_descriptors=[
            job_model.TaskDescriptor(
                task_metadata={
                    'task-id': 123,
                    'create-time': 123456
                },
                task_params=None,
                task_resources=None)
        ])

    poll_interval = 1
    ret = dsub_command._wait_and_retry(
        self.provider,
        'job-1',
        poll_interval,
        retries,
        job_descriptor,
        summary=False)
    tasks = self.provider.lookup_job_tasks({'*'})

    # First, the number of tasks returned by lookup_job_tasks should be equal
    # to the total number of task attempts.
    expected_num_of_tasks = len(list_of_list_of_operations[-1])
    self.assertEqual(len(tasks), expected_num_of_tasks)

    # Second, check that the return value of _wait_and_retry is correct.
    self.assertEqual(ret, expected)


if __name__ == '__main__':
  unittest.main()
