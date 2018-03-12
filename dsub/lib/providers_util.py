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

import textwrap
from . import job_model

_LOCALIZE_COMMAND_MAP = {
    job_model.P_GCS: 'gsutil -m rsync -r',
    job_model.P_LOCAL: 'rsync -r',
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
