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
"""Unit tests for the logging path parts of the local provider.
"""

import unittest
from dsub.providers import local


class TestLoggingPath(unittest.TestCase):

  def test_logging_path(self):
    jobid = 'job--id'
    test_cases = {
        # (--logging , jobid) , expected log prefix
        (('gs://bucket/file.log', jobid), 'gs://bucket/file'),
        (('gs://bucket/', jobid), 'gs://bucket/' + jobid),
        (('gs://bucket/folder', jobid), 'gs://bucket/folder/' + jobid),
        (('gs://bucket/isitafile.txt', jobid),
         'gs://bucket/isitafile.txt/' + jobid),
    }
    prov = local.LocalJobProvider()
    for t in test_cases:
      self.assertEqual(t[1], prov._make_logging_path(*t[0]))
