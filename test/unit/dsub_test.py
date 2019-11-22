# Copyright 2016 Google Inc. All Rights Reserved.
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
"""Unit tests for dsub."""

import doctest
import re
import unittest

import dsub as dsub_init
from dsub.commands import dsub as dsub_command
from dsub.providers import stub
import fake_time


def establish_chronology(chronology):
  dsub_command.SLEEP_FUNCTION = fake_time.FakeTime(chronology).sleep


def nothing_happens():
  yield 1


class TestWaitForAnyJob(unittest.TestCase):

  def progressive_chronology(self):
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }])
    yield 2
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('SUCCESS', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }])
    yield 1
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('SUCCESS', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('FAILURE', '123'),
        'status-message': '',
        'task-id': '',
    }])
    yield 1

  def test_already_succeeded(self):
    prov = stub.StubJobProvider()
    prov.set_operations([{
        'job-id': 'myjob',
        'status': 'SUCCESS',
        'status-message': '',
        'task-id': ''
    }])
    establish_chronology(nothing_happens())
    ret = dsub_command._wait_for_any_job(prov, {'myjob'}, 1, False)
    self.assertEqual(ret, set())

  def test_succeeds(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_for_any_job(self.prov, {'job-1'}, 1, False)
    self.assertEqual(ret, set())

  def test_fails(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_for_any_job(self.prov, {'job-2'}, 1, False)
    self.assertEqual(ret, set())

  def test_multiple_jobs(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_for_any_job(self.prov, {'job-1', 'job-2'}, 1,
                                         False)
    self.assertEqual(ret, {'job-2'})


class TestWaitForAnyJobBatch(unittest.TestCase):

  def progressive_chronology(self):
    self.prov.set_operations([{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }])
    yield 2
    self.prov.set_operations([{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'status': ('FAILURE', '123'),
        'status-message': '',
    }])
    yield 1

  def test_multiple_tasks(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_for_any_job(self.prov, {'job-1'}, 1, True)
    self.assertEqual(ret, set([]))


class TestWaitAfter(unittest.TestCase):

  def progressive_chronology(self):
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }])
    yield 2
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('SUCCESS', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
        'task-id': '',
    }])
    yield 1
    self.prov.set_operations([{
        'job-id': 'job-1',
        'status': ('SUCCESS', '123'),
        'status-message': '',
        'task-id': '',
    }, {
        'job-id': 'job-2',
        'status': ('FAILURE', '123'),
        'error-message': 'failed to frob',
        'status-message': '',
        'task-id': '',
    }])
    yield 1

  def test_already_succeeded(self):
    prov = stub.StubJobProvider()
    prov.set_operations([{
        'job-id': 'myjob',
        'status': ('SUCCESS', '123'),
        'status-message': '',
        'task-id': ''
    }])
    establish_chronology(nothing_happens())
    ret = dsub_command._wait_after(prov, ['myjob'], 1, True, False)
    self.assertEqual(ret, [])

  def test_job_not_found(self):
    prov = stub.StubJobProvider()
    prov.set_operations([{'job-id': 'myjob', 'status': ('SUCCESS', '123')}])
    establish_chronology(nothing_happens())
    ret = dsub_command._wait_after(prov, ['some_other_job'], 1, True, False)
    self.assertTrue(ret)

  def test_job_1(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_after(self.prov, ['job-1'], 1, True, False)
    self.assertEqual(ret, [])

  def test_job_2(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.progressive_chronology())
    ret = dsub_command._wait_after(self.prov, ['job-2'], 1, True, False)
    self.assertEqual(ret, [['failed to frob']])


class TestWaitAfterBatch(unittest.TestCase):

  def fail_in_sequence(self):
    self.prov.set_operations([{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }])
    yield 2
    self.prov.set_operations([{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'status': ('RUNNING', '123'),
        'status-message': '',
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'status': ('FAILURE', '123'),
        'error-message': 'failed to frob',
        'status-message': '',
    }])
    yield 1
    self.prov.set_operations([{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'status': ('FAILURE', '123'),
        'error-message': 'needs food badly',
        'status-message': '',
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'status': ('FAILURE', '123'),
        'error-message': 'failed to frob',
        'status-message': '',
    }])
    yield 1

  def test_job_2(self):
    self.prov = stub.StubJobProvider()
    establish_chronology(self.fail_in_sequence())
    ret = dsub_command._wait_after(self.prov, ['job-1'], 1, True, False)
    self.assertEqual(ret, [['failed to frob']])


class TestDominantTask(unittest.TestCase):

  def test_earliest_failure(self):
    ops = [{
        'job-id': 'job-1',
        'task-id': 'task-1',
        'end-time': 1,
        'status': ('SUCCESS', '1')
    }, {
        'job-id': 'job-1',
        'task-id': 'task-2',
        'end-time': 3,
        'status': ('FAILURE', '1')
    }, {
        'job-id': 'job-1',
        'task-id': 'task-3',
        'end-time': 2,
        'status': ('FAILURE', '1')
    }, {
        'job-id': 'job-1',
        'task-id': 'task-4',
        'end-time': 4,
        'status': ('FAILURE', '1')
    }, {
        'job-id': 'job-1',
        'task-id': 'task-5',
        'end-time': 5,
        'status': ('SUCCESS', '1')
    }]
    ops = [stub.StubTask(o) for o in ops]
    ret = dsub_command._dominant_task_for_jobs(ops)
    self.assertEqual(ret[0].get_field('task-id'), 'task-3')


class TestNameCommand(unittest.TestCase):

  def test_name_command(self):
    test_cases = {
        # command, name
        ('echo "hi"', 'echo'),
        ('\necho "hi"', 'echo'),
        ('\n  echo "hi"\n', 'echo'),
        ('\r\n  DIR\r\n', 'DIR'),
        ('samtools index "${BAM}"', 'samtools'),
        ('  \n  /bin/true', 'true'),
        ('/usr/bin/sort "${INFILE}" > "${OUTFILE}"', 'sort'),
        ('export VAR=val\necho ${VAR}', 'export'),
        ('# ignore\n  # ignore\n  export VAR=val\n echo ${VAR}', 'export'),
    }
    for t in test_cases:
      self.assertEqual(t[1], dsub_command._name_for_command(t[0]))


class TestExamplesInDocstrings(unittest.TestCase):

  def test_doctest(self):
    result = doctest.testmod(dsub_command, report=True)
    self.assertEqual(0, result.failed)


class TestDsubVersion(unittest.TestCase):

  # Find's any string containing "N.N". For example, "abc44.0100d" would pass.
  # Essentially this returns a hit for any "version-ish" string.
  VERSION_REGEX = r'\d{1,4}\.\d{1,4}'

  def test_init(self):
    self.assertTrue(hasattr(dsub_init, '__version__'))
    self.assertIsNotNone(re.search(self.VERSION_REGEX, dsub_init.__version__))


if __name__ == '__main__':
  unittest.main()
