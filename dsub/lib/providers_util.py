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
"""Helpers for providers."""

import os
import textwrap

from . import job_model
from .._dsub_version import DSUB_VERSION

_LOCALIZE_COMMAND_MAP = {
    job_model.P_GCS: 'gsutil -m rsync -r',
    job_model.P_LOCAL: 'rsync -r',
}

# Attempt to keep the dsub runtime environment sane by being prescriptive
# about what providers need to provide to the user's Docker container.
# Requirements can be found in the docs/providers/README.md.
#
# This module defines some utility names and functions such that new providers
# can follow the patterns of exising providers.
#
# Unless providers have a compelling reason not to, they should just provide
# a single disk for everything that needs to be written by the dsub
# runtime environment or the user.
#
# Backends like the Google Pipelines API, allow for the user to set both
# a boot-disk-size and a disk-size. But the boot-disk-size is not something
# that users should care about, so the Google providers put everything
# meaningful on the data disk:
#
#   input: files localized from object storage
#   output: files to de-localize to object storage
#
#   script: any code that dsub writes (like the user script)
#   tmp: set TMPDIR in the environment to point here
#
#   workingdir: A workspace directory for user code.
#               This is also the explicit working directory set before the
#               user script runs.

# Mount point for the data disk in the user's Docker container
DATA_MOUNT_POINT = '/mnt/data'

SCRIPT_DIR = '%s/script' % DATA_MOUNT_POINT
TMP_DIR = '%s/tmp' % DATA_MOUNT_POINT
WORKING_DIR = '%s/workingdir' % DATA_MOUNT_POINT


def get_file_environment_variables(file_params):
  """Return a dictionary of environment variables for the user container."""
  env = {}
  for param in file_params:
    # We have no cases where the environment variable provided to user
    # scripts have a trailing slash, so be sure to always strip it.
    # The case that this is specifically handling is --input-recursive and
    # --output-recursive variables, which are directory values.
    env[param.name] = os.path.join(
        DATA_MOUNT_POINT, param.docker_path.rstrip('/')) if param.value else ''
  return env


def build_recursive_localize_env(destination, inputs):
  """Return a multi-line string with export statements for the variables.

  Arguments:
    destination: Folder where the data will be put.
                 For example /mnt/data
    inputs: a list of InputFileParam

  Returns:
    a multi-line string with a shell script that sets environment variables
    corresponding to the inputs.
  """
  export_input_dirs = '\n'.join([
      'export {0}={1}/{2}'.format(var.name, destination.rstrip('/'),
                                  var.docker_path.rstrip('/'))
      for var in inputs
      if var.recursive and var.docker_path
  ])
  return export_input_dirs


def build_recursive_localize_command(destination, inputs, file_provider):
  """Return a multi-line string with a shell script to copy recursively.

  Arguments:
    destination: Folder where to put the data.
                 For example /mnt/data
    inputs: a list of InputFileParam
    file_provider: file provider string used to filter the output params; the
                 returned command will only apply outputs whose file provider
                 matches this file filter.

  Returns:
    a multi-line string with a shell script that copies the inputs
    recursively from GCS.
  """
  command = _LOCALIZE_COMMAND_MAP[file_provider]
  filtered_inputs = [
      var for var in inputs
      if var.recursive and var.file_provider == file_provider
  ]

  copy_input_dirs = '\n'.join([
      textwrap.dedent("""
      mkdir -p {data_mount}/{docker_path}
      for ((i = 0; i < 3; i++)); do
        if {command} {source_uri} {data_mount}/{docker_path}; then
          break
        elif ((i == 2)); then
          1>&2 echo "Recursive localization failed."
          exit 1
        fi
      done
      chmod -R o+r {data_mount}/{docker_path}
      """).format(
          command=command,
          source_uri=var.uri,
          data_mount=destination.rstrip('/'),
          docker_path=var.docker_path) for var in filtered_inputs
  ])
  return copy_input_dirs


def build_recursive_gcs_delocalize_env(source, outputs):
  """Return a multi-line string with export statements for the variables.

  Arguments:
    source: Folder with the data.
            For example /mnt/data
    outputs: a list of OutputFileParam

  Returns:
    a multi-line string with a shell script that sets environment variables
    corresponding to the outputs.
  """
  filtered_outs = [
      var for var in outputs
      if var.recursive and var.file_provider == job_model.P_GCS
  ]
  return '\n'.join([
      'export {0}={1}/{2}'.format(var.name,
                                  source.rstrip('/'),
                                  var.docker_path.rstrip('/'))
      for var in filtered_outs
  ])


def build_recursive_delocalize_command(source, outputs, file_provider):
  """Return a multi-line string with a shell script to copy recursively.

  Arguments:
    source: Folder with the data.
            For example /mnt/data
    outputs: a list of OutputFileParam.
    file_provider: file provider string used to filter the output params; the
                 returned command will only apply outputs whose file provider
                 matches this file filter.

  Returns:
    a multi-line string with a shell script that copies the inputs
    recursively to GCS.
  """
  command = _LOCALIZE_COMMAND_MAP[file_provider]
  filtered_outputs = [
      var for var in outputs
      if var.recursive and var.file_provider == file_provider
  ]

  return '\n'.join([
      textwrap.dedent("""
      for ((i = 0; i < 3; i++)); do
        if {command} {data_mount}/{docker_path} {destination_uri}; then
          break
        elif ((i == 2)); then
          1>&2 echo "Recursive de-localization failed."
          exit 1
        fi
      done
      """).format(
          command=command,
          data_mount=source.rstrip('/'),
          docker_path=var.docker_path,
          destination_uri=var.uri) for var in filtered_outputs
  ])


def get_task_metadata(job_metadata, task_id):
  """Returns a dict combining job metadata with the task id."""
  task_metadata = job_metadata.copy()
  task_metadata['task-id'] = task_id

  return task_metadata


def build_mount_env(source, mounts):
  """Return a multi-line string with export statements for the variables.

  Arguments:
    source: Folder with the data. For example /mnt/data
    mounts: a list of MountParam

  Returns:
    a multi-line string with a shell script that sets environment variables
    corresponding to the mounts.
  """
  return '\n'.join([
      'export {0}={1}/{2}'.format(var.name, source.rstrip('/'),
                                  var.docker_path.rstrip('/')) for var in mounts
  ])


def get_job_and_task_param(job_params, task_params, field):
  """Returns a dict combining the field for job and task params."""
  return job_params.get(field, set()) | task_params.get(field, set())


def prepare_job_metadata(script, job_name, user_id):
  """Returns a dictionary of metadata fields for the job."""

  # The name of the job is derived from the job_name and gets set as a
  # 'job-name' label (and so the value must be normalized).
  if not job_name:
    job_name = os.path.basename(script).split('.', 1)[0]
  job_name_value = job_model.convert_to_label_chars(job_name)

  # The user-id will get set as a label
  user_id = job_model.convert_to_label_chars(user_id)

  # Standard version is MAJOR.MINOR(.PATCH). This will convert the version
  # string to "vMAJOR-MINOR(-PATCH)". Example; "0.1.0" -> "v0-1-0".
  version = job_model.convert_to_label_chars('v%s' % DSUB_VERSION)
  return {
      'job-name': job_name_value,
      'user-id': user_id,
      'dsub-version': version,
  }
