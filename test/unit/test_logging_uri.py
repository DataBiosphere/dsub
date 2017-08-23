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
from dsub.lib import providers_util
import parameterized


class TestLoggingPath(unittest.TestCase):

  task_data_no_task = {
      'job-id': 'job--id',
      'job-name': 'job--name',
      'user-id': 'user--id',
  }
  task_data = {
      'job-id': 'job--id',
      'task-id': 'task--id',
      'job-name': 'job--name',
      'user-id': 'user--id',
  }

  @parameterized.parameterized.expand([
      # Directory paths
      ('1PG', 'gs://bucket/', task_data_no_task, 'gs://bucket/job--id.log'),
      ('1PL', '/tmp/dir/', task_data_no_task, '/tmp/dir/job--id.log'),
      ('2PG', 'gs://bucket/', task_data, 'gs://bucket/job--id.task--id.log'),
      ('2PL', '/tmp/dir/', task_data, '/tmp/dir/job--id.task--id.log'),
      ('3PG', 'gs://bucket/folder', task_data_no_task,
       'gs://bucket/folder/job--id.log'),
      ('3PL', '/tmp/dir/folder', task_data_no_task,
       '/tmp/dir/folder/job--id.log'),
      ('4PG', 'gs://bucket/folder', task_data,
       'gs://bucket/folder/job--id.task--id.log'),
      ('4PL', '/tmp/dir/folder', task_data,
       '/tmp/dir/folder/job--id.task--id.log'),
      ('5PG', 'gs://bucket/isitafile.txt', task_data_no_task,
       'gs://bucket/isitafile.txt/job--id.log'),
      ('5PL', '/tmp/dir/isitafile.txt', task_data_no_task,
       '/tmp/dir/isitafile.txt/job--id.log'),
      ('6PG', 'gs://bucket/isitafile.txt', task_data,
       'gs://bucket/isitafile.txt/job--id.task--id.log'),
      ('6PL', '/tmp/dir/isitafile.txt', task_data,
       '/tmp/dir/isitafile.txt/job--id.task--id.log'),

      # .log paths
      ('1FG', 'gs://bucket/file.log', task_data_no_task,
       'gs://bucket/file.log'),
      ('1FL', '/tmp/dir/file.log', task_data_no_task, '/tmp/dir/file.log'),
      ('2FG', 'gs://bucket/file.log', task_data,
       'gs://bucket/file.task--id.log'),
      ('2FL', '/tmp/dir/file.log', task_data, '/tmp/dir/file.task--id.log'),

      # Custom paths
      ('1CG', 'gs://bucket/path/{job-id}/{task-id}.log', task_data_no_task,
       'gs://bucket/path/job--id/task.log'),
      ('1CL', '/tmp/dir/path/{job-id}/{task-id}.log', task_data_no_task,
       '/tmp/dir/path/job--id/task.log'),
      ('2CG', 'gs://bucket/path/{job-id}/{task-id}.log', task_data,
       'gs://bucket/path/job--id/task--id.log'),
      ('2CL', '/tmp/dir/path/{job-id}/{task-id}.log', task_data,
       '/tmp/dir/path/job--id/task--id.log'),
      ('3CG', 'gs://bucket/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_data_no_task,
       'gs://bucket/path/user--id/job--name/job--id/task.log'),
      ('3CL', '/tmp/dir/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_data_no_task, '/tmp/dir/path/user--id/job--name/job--id/task.log'),
      ('4CG', 'gs://bucket/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_data, 'gs://bucket/path/user--id/job--name/job--id/task--id.log'),
      ('4CL', '/tmp/dir/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_data, '/tmp/dir/path/user--id/job--name/job--id/task--id.log'),
  ])
  def test_logging_path(self, unused_name, logging_uri, metadata, expected):
    formatted = providers_util.format_logging_uri(logging_uri, metadata)
    self.assertEqual(expected, formatted)
