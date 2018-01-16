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
"""Tests for dsub.lib.sorting_util."""

from dsub.lib import sorting_util
import unittest
import datetime
import parameterized


now = datetime.datetime.now()


def _desc_date_sort_key(task):
  return now - task['create-time'].replace(tzinfo=None)


JOB_STREAM_MULTI_1 = [
    {
        'job-id': 'test-job-1',
        'task-id': 'task-4',
        'create-time': datetime.datetime(2017, 11, 4, 10, 30, 50),
    },
    {
        'job-id': 'test-job-1',
        'task-id': 'task-3',
        'create-time': datetime.datetime(2017, 11, 4, 10, 30, 45),
    },
    {
        'job-id': 'test-job-1',
        'task-id': 'task-2',
        'create-time': datetime.datetime(2017, 11, 4, 10, 30, 40),
    },
    {
        'job-id': 'test-job-1',
        'task-id': 'task-1',
        'create-time': datetime.datetime(2017, 11, 4, 10, 30, 35),
    },
    {
        'job-id': 'test-job-2',
        'task-id': 'task-1',
        'create-time': datetime.datetime(2017, 11, 4, 6, 20, 10),
    },
]

JOB_STREAM_MULTI_2 = [
    {
        'job-id': 'test-job-3',
        'task-id': 'task-4',
        'create-time': datetime.datetime(2017, 11, 4, 12, 30, 50),
    },
    {
        'job-id': 'test-job-3',
        'task-id': 'task-3',
        'create-time': datetime.datetime(2017, 11, 4, 12, 30, 45),
    },
    {
        'job-id': 'test-job-3',
        'task-id': 'task-2',
        'create-time': datetime.datetime(2017, 11, 4, 12, 30, 40),
    },
    {
        'job-id': 'test-job-3',
        'task-id': 'task-1',
        'create-time': datetime.datetime(2017, 11, 4, 12, 30, 35),
    },
    {
        'job-id': 'test-job-4',
        'task-id': 'task-1',
        'create-time': datetime.datetime(2017, 11, 3, 6, 38, 10),
    },
]

JOB_STREAM_SINGLE = [
    {
        'job-id': 'test-job-4',
        'task-id': 'task-1',
        'create-time': datetime.datetime(2017, 11, 5, 6, 55, 10),
    },
]

JOB_STREAM_EMPTY = []

LAMBDA_KEYS = [
    ('create-time ascending', lambda t: t['create-time']),
    ('create-time descending', _desc_date_sort_key),
    ('job-od', lambda t: t['job-id']),
    ('task-id', lambda t: t['task-id']),
]


class SortedGeneratorIteratorTest(unittest.TestCase):

  @parameterized.parameterized.expand(LAMBDA_KEYS)
  def test_skips_empty_generator(self, unused_name, key):
    del unused_name
    stream_empty = _ListGenerator(sorted(JOB_STREAM_EMPTY, key=key))
    generator_iterator = sorting_util.SortedGeneratorIterator(key=key)
    self.assertIs(False,
                  generator_iterator.add_generator(stream_empty.generate()))
    self.assertIs(True, generator_iterator.empty())
    output = self.get_output(generator_iterator)
    self.assertEqual([], output)

  @parameterized.parameterized.expand(LAMBDA_KEYS)
  def test_handles_single(self, _, key):
    stream_empty = _ListGenerator(sorted(JOB_STREAM_EMPTY, key=key))
    stream_single = _ListGenerator(sorted(JOB_STREAM_SINGLE, key=key))
    generator_iterator = sorting_util.SortedGeneratorIterator(key=key)
    self.assertIs(False,
                  generator_iterator.add_generator(stream_empty.generate()))
    self.assertIs(True,
                  generator_iterator.add_generator(stream_single.generate()))
    output = self.get_output(generator_iterator)
    self.assertEqual(JOB_STREAM_SINGLE, output)

  @parameterized.parameterized.expand(LAMBDA_KEYS)
  def test_interleaves_multi(self, unused_name, key):
    del unused_name
    stream_1 = _ListGenerator(sorted(JOB_STREAM_MULTI_1, key=key))
    stream_2 = _ListGenerator(sorted(JOB_STREAM_MULTI_2, key=key))
    generator_iterator = sorting_util.SortedGeneratorIterator(key=key)
    self.assertIs(True, generator_iterator.add_generator(stream_1.generate()))
    self.assertIs(True, generator_iterator.add_generator(stream_2.generate()))

    output = self.get_output(generator_iterator)
    self.assertEqual(
        sorted(JOB_STREAM_MULTI_1 + JOB_STREAM_MULTI_2, key=key), output)

  @parameterized.parameterized.expand(LAMBDA_KEYS)
  def test_interleaves_all(self, unused_name, key):
    del unused_name
    stream_1 = _ListGenerator(sorted(JOB_STREAM_MULTI_1, key=key))
    stream_2 = _ListGenerator(sorted(JOB_STREAM_MULTI_2, key=key))
    stream_single = _ListGenerator(sorted(JOB_STREAM_SINGLE, key=key))
    stream_empty = _ListGenerator(sorted(JOB_STREAM_EMPTY, key=key))
    generator_iterator = sorting_util.SortedGeneratorIterator(key=key)
    self.assertIs(True, generator_iterator.add_generator(stream_1.generate()))
    self.assertIs(True, generator_iterator.add_generator(stream_2.generate()))
    self.assertIs(True,
                  generator_iterator.add_generator(stream_single.generate()))
    self.assertIs(False,
                  generator_iterator.add_generator(stream_empty.generate()))

    output = self.get_output(generator_iterator)
    self.assertEqual(
        sorted(
            JOB_STREAM_MULTI_1 + JOB_STREAM_MULTI_2 + JOB_STREAM_SINGLE,
            key=key), output)

  def get_output(self, generator_iterator):
    output = []
    for t in generator_iterator:
      output.append(t)
    self.assertIs(True, generator_iterator.empty())
    return output


class _ListGenerator(object):
  """Minimal wrapper around a list which simulates a generator"""

  def __init__(self, _list):
    self._list = _list

  def generate(self):
    for item in self._list:
      yield item


if __name__ == '__main__':
  unittest.main()
