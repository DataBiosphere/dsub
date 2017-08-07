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
from dsub.lib import param_util
from dsub.providers import local
import parameterized


class TestLoggingPath(unittest.TestCase):

  @parameterized.parameterized.expand([
      # Test name, logs path, logs basename, job id, task id, expected out.
      ('1G', 'gs://bucket/', 'file.log', 'jobid', None, 'gs://bucket/file'),
      ('1L', '/tmp/', 'file.log', 'jobid', None, '/tmp/file'),
      ('2G', 'gs://bucket/', '', 'jobid', None, 'gs://bucket/jobid'),
      ('2L', '/tmp/', '', 'jobid', None, '/tmp/jobid'),
      ('3G', 'gs://bk/folder/', '', 'jobid', None, 'gs://bk/folder/jobid'),
      ('3L', '/tmp/folder/', '', 'jobid', None, '/tmp/folder/jobid'),
      ('4G', 'gs://b/afile.txt/', '', 'jobid', None, 'gs://b/afile.txt/jobid'),
      ('5G', 'gs://b/', 'file.log', 'jobid', 'task', 'gs://b/file'),
      ('6G', 'gs://b/', '', 'jobid', 'taskid', 'gs://b/jobid.taskid'),
      ('7G', 'gs://b/dir/', '', 'jobid', 'taskid', 'gs://b/dir/jobid.taskid'),
      ('8G', 'gs://b/afile.txt/', '', 'jid', 'tid', 'gs://b/afile.txt/jid.tid'),
      # Some unexpected cases should still provide workable output.
      ('9G', 'gs://tmp/', '', None, 'unexpected', 'gs://tmp/None.unexpected'),
      ('9L', 'gs://tmp/', '', None, None, 'gs://tmp/None'),
      ('10G', 'gs://tmp/', '', '', '', 'gs://tmp/'),
  ])
  def test_logging_path(self, unused_name, path, logname, jid, tid, expected):
    prov = local.LocalJobProvider()
    loguri = param_util.UriParts(path, logname)
    self.assertEqual(expected, prov._prepare_logging_uri(loguri, jid, tid))
