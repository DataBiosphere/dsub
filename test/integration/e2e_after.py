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

"""Test calling dsub from python.

This test is a copy of the e2e_after.sh test with additional checks
on the objects returned by dsub.call().
"""

import os
import sys

from dsub.lib import dsub_errors

import test_setup_e2e as test
import test_util

TEST_FILE_PATH_1 = test.OUTPUTS + '/testfile1.txt'
TEST_FILE_PATH_2 = test.OUTPUTS + '/testfile2.txt'

if not os.environ.get('CHECK_RESULTS_ONLY'):
  print 'Launching pipeline...'

  # (1) Launch a simple job that should succeed after a short wait
  # pyformat: disable
  launched_job = test.run_dsub([
      '--command', 'sleep 5s && echo "hello world" > "${OUT}"',
      '--output', 'OUT=%s' % TEST_FILE_PATH_1])
  # pyformat: enable

  # (2) Wait for the previous job and then launch a new one that blocks
  # until exit
  # pyformat: disable
  next_job = test.run_dsub([
      '--after', launched_job['job-id'],
      '--input', 'IN=%s' % TEST_FILE_PATH_1,
      '--output', 'OUT=%s' % TEST_FILE_PATH_2,
      '--wait',
      '--command', 'cat "${IN}" > "${OUT}"'])
  # pyformat: enable

  # (3) Launch a job to test command execution failure
  try:
    # pyformat: disable
    bad_job_wait = test.run_dsub([
        '--command', 'exit 1',
        '--wait'])
    # pyformat: enable

    print >> sys.stderr, 'Expected to throw dsub_errors.JobExecutionError'
    sys.exit(1)
  except dsub_errors.JobExecutionError as e:
    if len(e.error_list) != 1:
      print >> sys.stderr, 'Expected 1 error during wait, got: %s' % (
          e.error_list)
      sys.exit(1)

  # (4) Launch a bad job to allow the next call to detect its failure
  # pyformat: disable
  bad_job_previous = test.run_dsub(['--command', 'sleep 5s && exit 1'])
  # pyformat: enable

  # (5) This call to dsub should fail before submit
  try:
    # pyformat: disable
    job_after = test.run_dsub([
        '--command', 'echo "does not matter"',
        '--after', bad_job_previous['job-id']])
    # pyformat: enable

    print >> sys.stderr, 'Expected to throw a PredecessorJobFailureError'
    sys.exit(1)
  except dsub_errors.PredecessorJobFailureError as e:
    if len(e.error_list) != 1:
      print >> sys.stderr, 'Expected 1 error from previous job, got: %s' % (
          e.error_list)
      sys.exit(1)

print
print 'Checking output...'

RESULT = test_util.gsutil_cat(TEST_FILE_PATH_2)
if 'hello world' not in RESULT:
  print 'Output file does not match expected'
  sys.exit(1)

print
print 'Output file matches expected:'
print '*****************************'
print RESULT
print '*****************************'

print 'SUCCESS'
