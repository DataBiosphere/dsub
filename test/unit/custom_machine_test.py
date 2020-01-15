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
"""Unit tests for GoogleV2CustomMachine class."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from dsub.providers import google_v2_base
import parameterized


class CustomMachineTest(unittest.TestCase):

  @parameterized.parameterized.expand([
      # (number of vCPUs, expected even number of vCPUs)
      (1, 1),  # 1 is the only acceptable odd number
      (2, 2),
      (3, 4),
      (4, 4),
      (5, 6),
      (6, 6),
  ])
  def test_validate_cores(self, input_cpu, expected_output):
    actual_output = google_v2_base.GoogleV2CustomMachine._validate_cores(
        input_cpu)
    self.assertEqual(actual_output, expected_output)

  @parameterized.parameterized.expand([
      # (memory in GB, expected rounded multiple of 256 MB)
      (1, 1024),
      (1.1, 1280),
      (1.25, 1280),
      (1.3, 1536),
      (1.5, 1536),
      (1.6, 1792),
      (1.75, 1792),
      (1.8, 2048),
      (2, 2048),
      (4, 4096),
      (10, 10240),
      (10.1, 10496),
      (10.25, 10496),
      (10.3, 10752),
      (10.5, 10752),
      (10.6, 11008),
      (10.75, 11008),
      (10.8, 11264),
  ])
  def test_validate_ram(self, input_ram, expected_output):
    actual_output = google_v2_base.GoogleV2CustomMachine._validate_ram(
        input_ram * google_v2_base.GoogleV2CustomMachine._MB_PER_GB)
    self.assertEqual(actual_output, expected_output)

  @parameterized.parameterized.expand([
      # (num vCPUs, memory in GB, expected custom machine type)
      (1, 1, 'custom-1-1024'),  # easy case
      (1, 4, 'custom-1-4096'),  # easy case
      (1, 3.9, 'custom-1-4096'),  # memory not a multiple of 256 MB
      (1, 4.1, 'custom-1-4352'),  # memory not a multiple of 256 MB
      (3, 4, 'custom-4-4096'),  # cpu not an even number
      (32, 29, 'custom-32-29696'),  # Satisfies min ratio requirement

      # Memory / cpu ratio must be > 0.9GB, increase memory
      (2, 1, 'custom-2-2048'),
      (3, 1, 'custom-4-3840'),
      (4, 1, 'custom-4-3840'),
      (16, 14, 'custom-16-14848'),

      # Memory / cpu ratio must be < 6.5GB, increase CPU
      (1, 6.6, 'custom-2-6912'),
      (1, 13.1, 'custom-4-13568'),
      (2, 13.1, 'custom-4-13568'),
      (3, 13.1, 'custom-4-13568'),
      (4, 26.1, 'custom-6-26880'),

      # Null checks
      (2, None, 'custom-2-3840'),
      (None, 1, 'custom-1-1024'),
      (None, None, 'custom-1-3840')
  ])
  def test_build_machine_type(self, min_cpu, min_ram, expected_output):
    custom_machine = google_v2_base.GoogleV2CustomMachine()
    actual_output = custom_machine.build_machine_type(min_cpu, min_ram)
    self.assertEqual(actual_output, expected_output)


if __name__ == '__main__':
  unittest.main()
