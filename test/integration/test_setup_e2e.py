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
"""Setup module for end-to-end dsub tests."""

# pylint: disable=line-too-long
# test_setup_e2e.py
#
# Intended to be imported into a test.
# Automatically imports variables from test_setup.py
#
# * Automatically determine PROJECT_ID
# * Automatically pick up a bucket name for tests.
#
# * Automatically set environment variables:
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/py/${DSUB_PROVIDER}/${TEST_NAME}/logging
#     (task file tests)
#   * LOGGING=gs://${DSUB_BUCKET}/dsub/py/${DSUB_PROVIDER}/${TEST_NAME}/logging/${TEST_NAME}.log
#     (non-task file tests)
#   * INPUTS=gs://${DSUB_BUCKET}/dsub/py/${DSUB_PROVIDER}/${TEST_NAME}/input
#   * OUTPUTS=gs://${DSUB_BUCKET}/dsub/py/${DSUB_PROVIDER}/${TEST_NAME}/output
#
# * Check if LOGGING, INPUTS, and OUTPUTS are empty.
# * For task file tests, generate the file from TASKS_FILE_TMPL.
# pylint: enable=line-too-long
from __future__ import print_function

import os
import subprocess
import sys

from dsub.commands import dsub as dsub_command

# Because this may be invoked from another directory (treated as a library) or
# invoked localy (treated as a binary) both import styles need to be supported.
# pylint: disable=g-import-not-at-top
try:
  from . import test_setup
  from . import test_util
except SystemError:
  import test_setup
  import test_util

TEST_VARS = ("TEST_NAME", "TEST_DIR", "TEST_TMP", "TASKS_FILE",
             "TASKS_FILE_TMPL",)
TEST_E2E_VARS = ("PROJECT_ID", "DSUB_BUCKET", "LOGGING", "INPUTS", "OUTPUTS",
                 "DOCKER_GCS_INPUTS", "DOCKER_GCS_OUTPUTS",)


def _environ():
  """Merge the current enviornment and test variables into a dictionary."""
  e = dict(os.environ)
  for var in TEST_VARS + TEST_E2E_VARS:
    e[var] = globals()[var]

  return e


# Copy test_setup variables
DSUB_PROVIDER = test_setup.DSUB_PROVIDER
TEST_NAME = test_setup.TEST_NAME
TEST_DIR = test_setup.TEST_DIR
TEST_TMP = test_setup.TEST_TMP
TASKS_FILE = test_setup.TASKS_FILE
TASKS_FILE_TMPL = test_setup.TASKS_FILE_TMPL

print("Checking that required environment values are set:")

if "YOUR_PROJECT" in os.environ:
  PROJECT_ID = os.environ["YOUR_PROJECT"]
else:
  print("Checking configured gcloud project")
  PROJECT_ID = subprocess.check_output(
      'gcloud config list core/project --format="value(core.project)"',
      shell=True,
      universal_newlines=True).strip()

if not PROJECT_ID:
  print("Your project ID could not be determined.")
  print("Set the environment variable YOUR_PROJECT or run \"gcloud init\".")
  sys.exit(1)

print("  Project ID detected as: %s" % PROJECT_ID)

if "YOUR_BUCKET" in os.environ:
  DSUB_BUCKET = os.environ["YOUR_BUCKET"]
else:
  DSUB_BUCKET = "%s-dsub-test" % os.environ["USER"]

print("  Bucket detected as: %s" % DSUB_BUCKET)

print("  Checking if bucket exists")
if not test_util.gsutil_ls_check("gs://%s" % DSUB_BUCKET):
  print("Bucket does not exist: %s" % DSUB_BUCKET, file=sys.stderr)
  print("Create the bucket with \"gsutil mb\".", file=sys.stderr)
  sys.exit(1)

# Set standard LOGGING, INPUTS, and OUTPUTS values
TEST_GCS_ROOT = "gs://%s/dsub/py/%s/%s" % (DSUB_BUCKET, DSUB_PROVIDER,
                                           TEST_NAME)
TEST_GCS_DOCKER_ROOT = "gs/%s/dsub/py/%s/%s" % (DSUB_BUCKET, DSUB_PROVIDER,
                                                TEST_NAME)

if TASKS_FILE:
  # For task file tests, the logging path is a directory.
  LOGGING = "%s/logging" % TEST_GCS_ROOT
else:
  # For regular tests, the logging path is a named file.
  LOGGING = TEST_GCS_ROOT + "/%s/logging/%s.log" % (TEST_NAME, TEST_NAME)
  STDOUT_LOG = "%s/%s-stdout.log" % (os.path.dirname(LOGGING), TEST_NAME)
  STDERR_LOG = "%s/%s-stderr.log" % (os.path.dirname(LOGGING), TEST_NAME)

INPUTS = "%s/input" % TEST_GCS_ROOT
OUTPUTS = "%s/output" % TEST_GCS_ROOT
DOCKER_GCS_INPUTS = "%s/input" % TEST_GCS_DOCKER_ROOT
DOCKER_GCS_OUTPUTS = "%s/output" % TEST_GCS_DOCKER_ROOT

print("Logging path: %s" % LOGGING)
print("Input path: %s" % INPUTS)
print("Output path: %s" % OUTPUTS)

print("  Checking if remote test files already exists")
if test_util.gsutil_ls_check("%s/**" % TEST_GCS_ROOT):
  print("Test files exist: %s" % TEST_GCS_ROOT, file=sys.stderr)
  print("Remove contents:", file=sys.stderr)
  print("  gsutil -m rm %s/**" % TEST_GCS_ROOT, file=sys.stderr)
  sys.exit(1)

if TASKS_FILE:
  # For a task file test, set up the task file from its template
  print("Setting up task file %s" % TASKS_FILE)
  if not os.path.exists(os.path.dirname(TASKS_FILE)):
    os.makedirs(os.path.dirname(TASKS_FILE))
  if os.path.exists(TASKS_FILE_TMPL):
    test_util.expand_tsv_fields(_environ(), TASKS_FILE_TMPL, TASKS_FILE)


# Functions for launching dsub
#
# Tests should generally just call "run_dsub" which will then invoke
# the provider-specific function.


def run_dsub(dsub_args):
  # Execute the appropriate dsub_<provider> function
  return globals()["dsub_%s" % DSUB_PROVIDER.replace("-", "_")](dsub_args)


def dsub_google_cls_v2(dsub_args):
  """Call dsub appending google-cls-v2 required arguments."""
  # pyformat: disable
  google_cls_v2_opt_args = [
      ("BOOT_DISK_SIZE", "--boot-disk-size"),
      ("DISK_SIZE", "--disk-size")
  ]
  # pyformat: enable

  opt_args = []
  for var in google_cls_v2_opt_args:
    val = globals().get(var[0])
    if val:
      opt_args.append(var[1], val)

  # pyformat: disable
  return dsub_command.call([
      "--provider", "google-cls-v2",
      "--project", PROJECT_ID,
      "--logging", LOGGING,
      "--regions", "us-central1"
      ] + opt_args + dsub_args)
  # pyformat: enable


def dsub_google_v2(dsub_args):
  """Call dsub appending google-v2 required arguments."""
  # pyformat: disable
  google_v2_opt_args = [
      ("BOOT_DISK_SIZE", "--boot-disk-size"),
      ("DISK_SIZE", "--disk-size")
  ]
  # pyformat: enable

  opt_args = []
  for var in google_v2_opt_args:
    val = globals().get(var[0])
    if val:
      opt_args.append(var[1], val)

  # pyformat: disable
  return dsub_command.call([
      "--provider", "google-v2",
      "--project", PROJECT_ID,
      "--logging", LOGGING,
      "--regions", "us-central1"
      ] + opt_args + dsub_args)
  # pyformat: enable


def dsub_local(dsub_args):
  """Call dsub appending local-provider required arguments."""

  # pyformat: disable
  return dsub_command.call([
      "--provider", "local",
      "--logging", LOGGING,
      ] + dsub_args)
