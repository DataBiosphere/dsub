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

This test is a copy of the e2e_env_list.sh test with additional checks
on the objects returned by dsub.call().
"""

import os
import sys

import test_setup_e2e as test
import test_util

if not os.environ.get('CHECK_RESULTS_ONLY'):
  print 'Launching pipeline...'

  # pyformat: disable
  launched_job = test.run_dsub([
      '--script', '%s/script_env_test.sh' % test.TEST_DIR,
      '--env', 'VAR1=VAL1', 'VAR2=VAL2', 'VAR3=VAL3',
      '--env', 'VAR4=VAL4',
      '--env', 'VAR5=VAL5',
      '--wait'])
  # pyformat: enable

  # Sanity check launched_jobs - should have a single record with no tasks
  if not launched_job:
    print >> sys.stderr, 'No launched jobs returned.'
    sys.exit(1)

  if not launched_job.get('job-id'):
    print >> sys.stderr, 'Launched job contains no job-id.'
    sys.exit(1)

  if not launched_job.get('user-id'):
    print >> sys.stderr, 'Launched job contains no user-id.'
    sys.exit(1)

  if launched_job.get('task-id'):
    print >> sys.stderr, 'Launched job contains tasks.'
    print >> sys.stderr, launched_job['task-id']
    sys.exit(1)

  print 'Launched job: %s' % launched_job['job-id']

print
print 'Checking output...'

# Check the results
RESULT_EXPECTED = """
VAR1=VAL1
VAR2=VAL2
VAR3=VAL3
VAR4=VAL4
VAR5=VAL5
""".lstrip()

RESULT = test_util.gsutil_cat(test.STDOUT_LOG)
if not test_util.diff(RESULT_EXPECTED, RESULT):
  print 'Output file does not match expected'
  sys.exit(1)

print
print 'Output file matches expected:'
print '*****************************'
print RESULT
print '*****************************'

print 'SUCCESS'
