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
"""Unit tests for parse_rfc3339_utc_string function."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from dsub.providers import google_base
import parameterized


class Rfc3339Test(unittest.TestCase):

  @parameterized.parameterized.expand([
      ('2019-10-08T12:11:24.999999594Z', '2019-10-08 12:11:24.999999+00:00'),
      ('2016-11-14T23:05:56Z', '2016-11-14 23:05:56+00:00'),
      ('2016-11-14T23:05:56.010Z', '2016-11-14 23:05:56.010000+00:00'),
      ('2016-11-14T23:05:56.010429Z', '2016-11-14 23:05:56.010429+00:00'),
      ('2016-11-14T23:05:56.010429380Z', '2016-11-14 23:05:56.010429+00:00')
  ])
  def test_parse_rfc3339_utc_string(self, input_utc_string, expected_output):
    datetime_object = google_base.parse_rfc3339_utc_string(input_utc_string)
    self.assertEqual(str(datetime_object), expected_output)


if __name__ == '__main__':
  unittest.main()
