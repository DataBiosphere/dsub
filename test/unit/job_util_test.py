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
"""Tests for lib.job_util."""

from __future__ import absolute_import

import parameterized
import unittest

from dsub.lib import job_util


class JobUtilTest(unittest.TestCase):

  DEFAULT_RESOURCES = {
      'min_cores': 1,
      'min_ram': 3.75,
      'disk_size': 200,
      'boot_disk_size': 10,
      'preemptible': False,
      'image': None,
      'logging': None,
      'zones': None,
      'scopes': ['https://www.googleapis.com/auth/bigquery',],
      'accelerator_type': None,
      'accelerator_count': 0,
  }

  USER_RESOURCES = job_util.JobResources(
      zones='us-east1-a,us-central1-a',
      min_ram=16,
      image='my-docker-img',
      min_cores=8,
      accelerator_type='nvidia-tesla-k80',
      accelerator_count=2)

  EXPECTED_USER_RESOURCES = {
      'min_cores': 8,
      'min_ram': 16,
      'disk_size': 200,
      'boot_disk_size': 10,
      'preemptible': False,
      'image': 'my-docker-img',
      'logging': None,
      'zones': 'us-east1-a,us-central1-a',
      'scopes': ['https://www.googleapis.com/auth/bigquery',],
      'accelerator_type': 'nvidia-tesla-k80',
      'accelerator_count': 2,
  }

  @parameterized.parameterized.expand(
      [('test default resource creation', job_util.JobResources(),
        DEFAULT_RESOURCES), ('test user defined job resources', USER_RESOURCES,
                             EXPECTED_USER_RESOURCES)])
  def test_job_resource_creation(self, unused_name, resource, expected):
    del unused_name
    for name, value in expected.iteritems():
      self.assertEqual(value, getattr(resource, name))

  def testScriptCreation(self):
    script = job_util.Script('the-name', 'the-value')
    self.assertEqual('the-name', script.name)
    self.assertEqual('the-value', script.value)


if __name__ == '__main__':
  unittest.main()
