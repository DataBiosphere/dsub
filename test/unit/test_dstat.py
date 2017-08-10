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
"""Tests for dsub.lib.dsub_util."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from datetime import datetime
import unittest
from dsub.commands import dstat as dstat_command
import parameterized


class TestDstat(unittest.TestCase):

  fixed_time = datetime(2017, 1, 1)
  fixed_time_utc = int(
      (fixed_time - datetime.utcfromtimestamp(0)).total_seconds())

  @parameterized.parameterized.expand([
      ('simple_second', '1s', fixed_time_utc - 1),
      ('simple_minute', '1m', fixed_time_utc - (1 * 60)),
      ('simple_hour', '1h', fixed_time_utc - (1 * 60 * 60)),
      ('simple_day', '1d', fixed_time_utc - (24 * 60 * 60)),
      ('simple_week', '1w', fixed_time_utc - (7 * 24 * 60 * 60)),
      ('simple_now', str(fixed_time_utc), fixed_time_utc),
  ])
  def test_compute_create_time(self, unused_name, age, expected):
    result = dstat_command.compute_create_time(age, self.fixed_time)
    self.assertEqual(expected, result)

  @parameterized.parameterized.expand([
      ('bad_units', '1second'),
      ('overflow', '100000000w'),
  ])
  def test_compute_create_time_fail(self, unused_name, age):
    with self.assertRaisesRegexp(ValueError, 'Unable to parse age string'):
      _ = dstat_command.compute_create_time(age)


if __name__ == '__main__':
  unittest.main()
