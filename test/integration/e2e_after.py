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

"""Test the --after and --wait flags when dsub jobs succeed.

This test is a copy of the e2e_after.sh test with additional checks
on the objects returned by dsub.call().
"""
from __future__ import print_function

import os
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

TEST_FILE_PATH_1 = test.OUTPUTS + '/testfile1.txt'
TEST_FILE_PATH_2 = test.OUTPUTS + '/testfile2.txt'

if not os.environ.get('CHECK_RESULTS_ONLY'):

  # (1) Launch a simple job that should succeed after a short wait
  print('Launch a job (and don\'t --wait)...')
  # pyformat: disable
  launched_job = test.run_dsub([
      '--command', 'sleep 5s && echo "hello world" > "${OUT}"',
      '--output', 'OUT=%s' % TEST_FILE_PATH_1])
  # pyformat: enable

  # (2) Wait for the previous job and then launch a new one that blocks
  # until exit
  print('Launch a job (--after the previous, and then --wait)...')
  # pyformat: disable
  next_job = test.run_dsub([
      '--after', launched_job['job-id'],
      '--input', 'IN=%s' % TEST_FILE_PATH_1,
      '--output', 'OUT=%s' % TEST_FILE_PATH_2,
      '--wait',
      '--command', 'cat "${IN}" > "${OUT}"'])
  # pyformat: enable

print('\nChecking output...')

RESULT = test_util.gsutil_cat(TEST_FILE_PATH_2)
if 'hello world' not in RESULT:
  print('Output file does not match expected', file=sys.stderr)
  sys.exit(1)

print('\nOutput file matches expected:')
print('*****************************')
print(RESULT)
print('*****************************')

print('SUCCESS')
