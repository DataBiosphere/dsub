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

"""Test the --after and --wait flags when dsub jobs fail.

This test is a copy of the e2e_after_fail.sh test with additional checks
on the objects returned by dsub.call().
"""

import os
import sys

from dsub.lib import dsub_errors

import test_setup_e2e as test

if not os.environ.get('CHECK_RESULTS_ONLY'):

  # (1) Launch a job to test command execution failure
  print 'Launch a job that should fail (--wait for it)...'
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

  # (2) Launch a bad job to allow the next call to detect its failure
  print 'Launch a job that should fail (don\'t --wait)'
  # pyformat: disable
  bad_job_previous = test.run_dsub(['--command', 'sleep 5s && exit 1'])
  # pyformat: enable

  # (3) This call to dsub should fail before submit
  print 'Launch a job that should fail (--after the previous)'
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
