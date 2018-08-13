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
from dsub.providers import provider_base
import parameterized


class TestLoggingPath(unittest.TestCase):

  job_metadata = {
      'job-id': 'job--id',
      'job-name': 'job--name',
      'user-id': 'user--id',
  }
  task_metadata_no_task = {}
  task_metadata = {
      'task-id': 'task--id',
  }
  task_metadata_attempt = {
      'task-id': 'task--id',
      'task-attempt': 'task--attempt',
  }

  @parameterized.parameterized.expand([
      # Directory paths
      ('1PG', 'gs://bucket/', task_metadata_no_task, 'gs://bucket/job--id.log'),
      ('1PL', '/tmp/dir/', task_metadata_no_task, '/tmp/dir/job--id.log'),
      ('2PG', 'gs://bucket/', task_metadata,
       'gs://bucket/job--id.task--id.log'),
      ('2PL', '/tmp/dir/', task_metadata, '/tmp/dir/job--id.task--id.log'),
      ('3PG', 'gs://bucket/folder', task_metadata_no_task,
       'gs://bucket/folder/job--id.log'),
      ('3PL', '/tmp/dir/folder', task_metadata_no_task,
       '/tmp/dir/folder/job--id.log'),
      ('4PG', 'gs://bucket/folder', task_metadata,
       'gs://bucket/folder/job--id.task--id.log'),
      ('4PL', '/tmp/dir/folder', task_metadata,
       '/tmp/dir/folder/job--id.task--id.log'),
      ('5PG', 'gs://bucket/isitafile.txt', task_metadata_no_task,
       'gs://bucket/isitafile.txt/job--id.log'),
      ('5PL', '/tmp/dir/isitafile.txt', task_metadata_no_task,
       '/tmp/dir/isitafile.txt/job--id.log'),
      ('6PG', 'gs://bucket/isitafile.txt', task_metadata,
       'gs://bucket/isitafile.txt/job--id.task--id.log'),
      ('6PL', '/tmp/dir/isitafile.txt', task_metadata,
       '/tmp/dir/isitafile.txt/job--id.task--id.log'),
      ('7PG', 'gs://bucket/isitafile.txt', task_metadata_attempt,
       'gs://bucket/isitafile.txt/job--id.task--id.task--attempt.log'),
      ('7PL', '/tmp/dir/isitafile.txt', task_metadata_attempt,
       '/tmp/dir/isitafile.txt/job--id.task--id.task--attempt.log'),

      # .log paths
      ('1FG', 'gs://bucket/file.log', task_metadata_no_task,
       'gs://bucket/file.log'),
      ('1FL', '/tmp/dir/file.log', task_metadata_no_task, '/tmp/dir/file.log'),
      ('2FG', 'gs://bucket/file.log', task_metadata,
       'gs://bucket/file.task--id.log'),
      ('2FL', '/tmp/dir/file.log', task_metadata, '/tmp/dir/file.task--id.log'),
      ('3FG', 'gs://bucket/file.log', task_metadata_attempt,
       'gs://bucket/file.task--id.task--attempt.log'),
      ('3FL', '/tmp/dir/file.log', task_metadata_attempt,
       '/tmp/dir/file.task--id.task--attempt.log'),

      # Custom paths
      ('1CG', 'gs://bucket/path/{job-id}/{task-id}.log', task_metadata_no_task,
       'gs://bucket/path/job--id/task.log'),
      ('1CL', '/tmp/dir/path/{job-id}/{task-id}.log', task_metadata_no_task,
       '/tmp/dir/path/job--id/task.log'),
      ('2CG', 'gs://bucket/path/{job-id}/{task-id}.log', task_metadata,
       'gs://bucket/path/job--id/task--id.log'),
      ('2CL', '/tmp/dir/path/{job-id}/{task-id}.log', task_metadata,
       '/tmp/dir/path/job--id/task--id.log'),
      ('3CG', 'gs://bucket/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_metadata_no_task,
       'gs://bucket/path/user--id/job--name/job--id/task.log'),
      ('3CL', '/tmp/dir/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_metadata_no_task,
       '/tmp/dir/path/user--id/job--name/job--id/task.log'),
      ('4CG', 'gs://bucket/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_metadata,
       'gs://bucket/path/user--id/job--name/job--id/task--id.log'),
      ('4CL', '/tmp/dir/path/{user-id}/{job-name}/{job-id}/{task-id}.log',
       task_metadata, '/tmp/dir/path/user--id/job--name/job--id/task--id.log'),
      ('5CG', 'gs://bucket/path/{user-id}/{job-name}/{job-id}/{task-id}.'
       '{task-attempt}.log', task_metadata_attempt,
       'gs://bucket/path/user--id/job--name/job--id/task--id.task--attempt.log'
      ),
      ('5CL', '/tmp/dir/path/{user-id}/{job-name}/{job-id}/{task-id}.'
       '{task-attempt}.log', task_metadata_attempt,
       '/tmp/dir/path/user--id/job--name/job--id/task--id.task--attempt.log'),
  ])
  def test_logging_path(self, unused_name, logging_uri, task_metadata,
                        expected):
    del unused_name
    formatted = provider_base.format_logging_uri(logging_uri, self.job_metadata,
                                                 task_metadata)
    self.assertEqual(expected, formatted)


if __name__ == '__main__':
  unittest.main()
