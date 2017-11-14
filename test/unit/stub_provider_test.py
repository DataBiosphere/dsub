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
"""Unit tests for the stub provider.
"""

import unittest
from dsub.providers import stub


def raw_ops(tasklist):
  """Convert returned operations to raw operations."""
  return [t.raw_task_data() for t in tasklist]


class TestGetJob(unittest.TestCase):

  def test_get_success(self):
    prov = stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.lookup_job_tasks(['SUCCESS'])
    self.assertEqual(raw_ops(tasks), [job_suc])

  def test_get_several(self):
    prov = stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    job_run = {'job-id': 'job_run', 'status': ('RUNNING', '123')}
    prov.set_operations([job_suc, job_fail, job_run])
    tasks = prov.lookup_job_tasks(['SUCCESS', 'FAILURE'])
    self.assertEqual(raw_ops(tasks), [job_suc, job_fail])

  def test_get_star_set(self):
    prov = stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.lookup_job_tasks({'*'})
    self.assertEqual(raw_ops(tasks), [job_suc, job_fail])

  def test_get_none(self):
    prov = stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.lookup_job_tasks(None)
    self.assertEqual(raw_ops(tasks), [job_suc, job_fail])


if __name__ == '__main__':
  unittest.main()
