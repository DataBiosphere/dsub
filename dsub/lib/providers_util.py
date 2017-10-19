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
from . import param_util

_LOCALIZE_COMMAND_MAP = {
    param_util.P_GCS: 'gsutil -m rsync -r',
    param_util.P_LOCAL: 'rsync -r',
}


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
      'export {0}={1}/{2}'.format(var.name,
                                  destination.rstrip('/'),
                                  var.docker_path.rstrip('/')) for var in inputs
      if var.recursive
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
          2>&1 echo "Recursive localization failed."
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
      if var.recursive and var.file_provider == param_util.P_GCS
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
          2>&1 echo "Recursive de-localization failed."
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


def _format_task_uri(fmt, task_metadata):
  """Returns a URI with placeholders replaced by task metadata values."""

  values = {
      'job-id': None,
      'task-id': 'task',
      'job-name': None,
      'user-id': None
  }
  for key in values:
    values[key] = task_metadata.get(key) or values[key]
  return fmt.format(**values)


def format_logging_uri(uri, task_metadata):
  """Inserts task metadata into the logging URI.

  The core behavior is inspired by the Google Pipelines API:
    (1) If a the uri ends in ".log", then that is the logging path.
    (2) Otherwise, the uri is treated as "directory" for logs and a filename
        needs to be automatically generated.

  For (1), if the job is a --tasks job, then the {task-id} is inserted
  before ".log".

  For (2), the file name generated is {job-id}, or for --tasks jobs, it is
  {job-id}.{task-id}.

  In addition, full task metadata subsitition is supported. The URI
  may include substitution strings such as
  "{job-id}", "{task-id}", "{job-name}", and "{user-id}".

  Args:
    uri: URI indicating where to write logs
    task_metadata: dictionary of task metadata values

  Returns:
    The logging_uri formatted as described above.
  """

  task_id = task_metadata.get('task-id')

  # If the user specifies any formatting (with curly braces), then use that
  # as the format string unchanged.
  fmt = str(uri)
  if '{' not in fmt:
    if uri.endswith('.log'):
      if task_id is not None:
        parts = os.path.splitext(uri)
        fmt = '%s.{task-id}.log' % parts[0]
    else:
      # The path is a directory - generate the file name
      if task_id is not None:
        fmt = os.path.join(uri, '{job-id}.{task-id}.log')
      else:
        fmt = os.path.join(uri, '{job-id}.log')

  return _format_task_uri(fmt, task_metadata)
