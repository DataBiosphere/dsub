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
from __future__ import print_function

import sys

# Because this may be invoked from another directory (treated as a library) or
# invoked localy (treated as a binary) both import styles need to be supported.
# pylint: disable=g-import-not-at-top
try:
  from . import test_setup_e2e as test
  from . import test_util
except SystemError:
  import test_setup_e2e as test
  import test_util

print('Launching pipeline...')

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
  print('No launched jobs returned.', file=sys.stderr)
  sys.exit(1)

if not launched_job.get('job-id'):
  print('Launched job contains no job-id.', file=sys.stderr)
  sys.exit(1)

if not launched_job.get('user-id'):
  print('Launched job contains no user-id.', file=sys.stderr)
  sys.exit(1)

if launched_job.get('task-id'):
  print('Launched job contains tasks.', file=sys.stderr)
  print(launched_job['task-id'], file=sys.stderr)
  sys.exit(1)

print('Launched job: %s' % launched_job['job-id'])

print('\nChecking output...')

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
  print('Output file does not match expected')
  sys.exit(1)

print('\nOutput file matches expected:')
print('*****************************')
print(RESULT)
print('*****************************')

print('SUCCESS')
