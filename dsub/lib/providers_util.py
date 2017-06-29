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


def build_recursive_gcs_localize_command(destination, inputs):
  """Return a multi-line string with a shell script to copy recursively.

  Arguments:
    destination: Folder where to put the data.
                 For example /mnt/data
    inputs: a list of InputFileParam

  Returns:
    a multi-line string with a shell script that copies the inputs
    recursively from GCS.
  """
  copy_input_dirs = '\n'.join([
      textwrap.dedent("""
      mkdir -p {1}/{2}
      for ((i = 0; i < 3; i++)); do
        if gsutil -m rsync -r {0} {1}/{2}; then
          break
        elif ((i == 2)); then
          2>&1 echo "Recursive localization failed."
          exit 1
        fi
      done
      chmod -R o+r {1}/{2}
      """).format(var.remote_uri, destination.rstrip('/'), var.docker_path)
      for var in inputs if var.recursive
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
  return '\n'.join([
      'export {0}={1}/{2}'.format(var.name,
                                  source.rstrip('/'),
                                  var.docker_path.rstrip('/'))
      for var in outputs if var.recursive
  ])


def build_recursive_gcs_delocalize_command(source, outputs):
  """Return a multi-line string with a shell script to copy recursively.

  Arguments:
    source: Folder with the data.
            For example /mnt/data
    outputs: a list of OutputFileParam

  Returns:
    a multi-line string with a shell script that copies the inputs
    recursively to GCS.
  """
  return '\n'.join([
      textwrap.dedent("""
      for ((i = 0; i < 3; i++)); do
        if gsutil -m rsync -r {0}/{1} {2}; then
          break
        elif ((i == 2)); then
          2>&1 echo "Recursive de-localization failed."
          exit 1
        fi
      done
      """).format(source.rstrip('/'), var.docker_path, var.remote_uri)
      for var in outputs if var.recursive
  ])
