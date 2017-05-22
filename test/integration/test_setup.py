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
# * Set the TEST_NAME based on the name of the calling script.
# * Set variables for TEST_DIR.
# * For task file tests, set TASKS_FILE and TASKS_FILE_TMPL.
# * Set the TEST_TEMP variable for a temporary directory.

import os
import sys

# Compute the name of the test from the calling script
# (trim the e2e_ or unit_ prefix, along with the .py extension)
TEST_NAME = os.path.splitext(os.path.basename(sys.argv[0]).split('_', 1)[1])[0]

print 'Setting up test: %s' % TEST_NAME

# Set up the path to dsub.py
TEST_DIR = os.path.dirname(sys.argv[0])

if TEST_NAME.endswith('_tasks'):
  TASKS_FILE_TMPL = '%s/%s.tsv.tmpl' % (TEST_DIR, TEST_NAME)
  TASKS_FILE = '%s/%s.tsv' % (TEST_DIR, TEST_NAME)
else:
  TASKS_FILE_TMPL = None
  TASKS_FILE = None

TEST_TEMP = '%s/%s' % (TEST_DIR, '_tmp')
