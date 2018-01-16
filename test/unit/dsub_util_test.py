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
import os
import unittest
from dsub.lib import dsub_util


class TestDsubUtil(unittest.TestCase):

  def testLoadFile(self):
    testpath = os.path.dirname(__file__)
    tsv_file = os.path.join(testpath, '../testdata/params_tasks.tsv')
    self.assertTrue(dsub_util.load_file(tsv_file))


if __name__ == '__main__':
  unittest.main()
