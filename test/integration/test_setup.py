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
"""Setup module for dsub tests."""

# test_setup.py
#
# Intended to be imported into a test.
# The code here will:
#
# * Ensure the DSUB_PROVIDER is set (default: local)
# * Set the TEST_NAME based on the name of the calling script.
# * Set the TEST_DIR to the directory the test file is in.
# * For task file tests, set TASKS_FILE and TASKS_FILE_TMPL.
# * Set the TEST_TMP variable for a temporary directory.
from __future__ import print_function

import datetime
import os
import random
import string
import sys

# If the DSUB_PROVIDER is not set, figure it out from the name of the script.
#   If the script name is <test>.<provider>.sh, pull out the provider.
#   If the script name is <test>.sh, use "local".
# If the DSUB_PROVIDER is set, make sure it is correct for a provider test.

SCRIPT_NAME = os.path.basename(sys.argv[0])
SCRIPT_DEFAULT_PROVIDER = SCRIPT_NAME.split('.')[1] if SCRIPT_NAME.count(
    '.') == 2 else None

DSUB_PROVIDER = os.getenv('DSUB_PROVIDER')
if not DSUB_PROVIDER:
  if SCRIPT_DEFAULT_PROVIDER:
    DSUB_PROVIDER = SCRIPT_DEFAULT_PROVIDER
  else:
    DSUB_PROVIDER = 'local'
elif SCRIPT_DEFAULT_PROVIDER:
  if DSUB_PROVIDER != SCRIPT_DEFAULT_PROVIDER:
    print('DSUB_PROVIDER inconsistent with default provider', file=sys.stderr)
    print('"%s" is not "%s"' % (DSUB_PROVIDER, SCRIPT_DEFAULT_PROVIDER),
          file=sys.stderr)
    sys.exit(1)

# Compute the name of the test from the calling script
# (trim the e2e_ or unit_ prefix, along with the .py extension)
TEST_NAME = os.path.splitext(SCRIPT_NAME.split('_', 1)[1])[0]

print('Setting up test: %s' % TEST_NAME)

TEST_DIR = os.path.dirname(sys.argv[0])

TEST_TMP = '%s/tmp' % os.getenv('TEST_TMP', '/tmp/dsub-test/py/%s/%s' %
                                (DSUB_PROVIDER, TEST_NAME))

if TEST_NAME.endswith('_tasks'):
  TASKS_FILE_TMPL = '%s/%s.tsv.tmpl' % (TEST_DIR, TEST_NAME)
  TASKS_FILE = '%s/%s.tsv' % (TEST_TMP, TEST_NAME)
else:
  TASKS_FILE_TMPL = None
  TASKS_FILE = None


def _generate_test_token():
  # Generate an id for tests to use that is reasonably likely to be unique
  # (timestamp + 8 random characters).
  timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
  suffix = ''.join(
      random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
  return '{}_{}'.format(timestamp, suffix)


TEST_TOKEN = os.getenv('TEST_TOKEN', _generate_test_token())
