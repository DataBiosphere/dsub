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

import unittest

from dsub.lib import job_util


class JobUtilTest(unittest.TestCase):

  def check_resources(self, expected_resource, actual_job_resource):
    for name, value in expected_resource.iteritems():
      self.assertEqual(value, getattr(actual_job_resource, name))

  def testJobResourceCreation(self):
    # Default job resources.
    resource = job_util.JobResources()
    self.check_resources({
        'min_cores': 1,
        'min_ram': 1,
        'disk_size': 10,
        'boot_disk_size': 10,
        'preemptible': False,
        'image': None,
        'logging': None,
        'zones': None,
        'scopes': None
    }, resource)

    # User defined job resources.
    resource = job_util.JobResources(
        zones='us-east1-a,us-central1-a',
        min_ram=16,
        image='my-docker-img',
        min_cores=8)
    self.check_resources({
        'min_cores': 8,
        'min_ram': 16,
        'disk_size': 10,
        'boot_disk_size': 10,
        'preemptible': False,
        'image': 'my-docker-img',
        'logging': None,
        'zones': 'us-east1-a,us-central1-a',
        'scopes': None
    }, resource)

  def testScriptCreation(self):
    script = job_util.Script('the-name', 'the-value')
    self.assertEqual('the-name', script.name)
    self.assertEqual('the-value', script.value)


if __name__ == '__main__':
  unittest.main()
