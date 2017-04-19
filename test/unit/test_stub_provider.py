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
import providers.stub


class TestGetJob(unittest.TestCase):

  def test_get_success(self):
    prov = providers.stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.get_jobs(['SUCCESS'])
    self.assertEqual(tasks, [job_suc])

  def test_get_several(self):
    prov = providers.stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    job_run = {'job-id': 'job_run', 'status': ('RUNNING', '123')}
    prov.set_operations([job_suc, job_fail, job_run])
    tasks = prov.get_jobs(['SUCCESS', 'FAILURE'])
    self.assertEqual(tasks, [job_suc, job_fail])

  def test_get_star(self):
    prov = providers.stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.get_jobs('*')
    self.assertEqual(tasks, [job_suc, job_fail])

  def test_get_star_list(self):
    prov = providers.stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.get_jobs(['*'])
    self.assertEqual(tasks, [job_suc, job_fail])

  def test_get_none(self):
    prov = providers.stub.StubJobProvider()
    job_suc = {'job-id': 'job_suc', 'status': ('SUCCESS', '123')}
    job_fail = {'job-id': 'job_fail', 'status': ('FAILURE', '123')}
    prov.set_operations([job_suc, job_fail])
    tasks = prov.get_jobs(None)
    self.assertEqual(tasks, [job_suc, job_fail])


if __name__ == '__main__':
  unittest.main()
