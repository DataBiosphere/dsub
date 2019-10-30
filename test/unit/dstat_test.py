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
"""Unit tests for dstat.

Run it like this:

python dstat_test.py
"""

import collections
import unittest

from dsub.lib import output_formatter


class TestSummarize(unittest.TestCase):
  """Tests for the _prepare_summary_table function in dstat."""

  def test_summarize_can_count(self):
    fake_tasks_table = [{
        'job-name': 'job1',
        'status': 'RUNNING'
    }, {
        'job-name': 'job1',
        'status': 'RUNNING'
    }]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'RUNNING'),
                                 ('task-count', 2)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)

  def test_summarize_removes_extra_fields(self):
    fake_tasks_table = [{
        'job-name': 'job1',
        'status': 'RUNNING',
        'favorite color': 'pink'
    }]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'RUNNING'),
                                 ('task-count', 1)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)

  def test_summarize_keeps_jobs_separate(self):
    fake_tasks_table = [{
        'job-name': 'job1',
        'status': 'RUNNING'
    }, {
        'job-name': 'job2',
        'status': 'RUNNING'
    }]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'RUNNING'),
                                 ('task-count', 1)]),
        collections.OrderedDict([('job-name', 'job2'), ('status', 'RUNNING'),
                                 ('task-count', 1)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)

  def test_summarize_keeps_status_separate(self):
    fake_tasks_table = [{
        'job-name': 'job1',
        'status': 'RUNNING'
    }, {
        'job-name': 'job1',
        'status': 'FAILURE'
    }]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'RUNNING'),
                                 ('task-count', 1)]),
        collections.OrderedDict([('job-name', 'job1'), ('status', 'FAILURE'),
                                 ('task-count', 1)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)

  def test_summarize_puts_running_before_failure(self):
    fake_tasks_table = [{
        'job-name': 'job1',
        'status': 'FAILURE'
    }, {
        'job-name': 'job1',
        'status': 'RUNNING'
    }]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'RUNNING'),
                                 ('task-count', 1)]),
        collections.OrderedDict([('job-name', 'job1'), ('status', 'FAILURE'),
                                 ('task-count', 1)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)

  def test_summarize_keeps_nonstandard_status(self):
    fake_tasks_table = [{'job-name': 'job1', 'status': 'FLYING'}]
    expected_table = [
        collections.OrderedDict([('job-name', 'job1'), ('status', 'FLYING'),
                                 ('task-count', 1)])
    ]
    table = output_formatter.prepare_summary_table(fake_tasks_table)
    self.assertEqual(table, expected_table)


if __name__ == '__main__':
  unittest.main()
