# Copyright 2022 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility functions to be used by the Google providers.

This module holds constants and methods useful to google-cls-v2
and google-batch providers.
"""
import os
import textwrap
from typing import Dict

from . import base

from ..lib import job_model
from ..lib import providers_util

STATUS_FILTER_MAP = {
    'RUNNING': 'status.state="RUNNING" OR status.state="QUEUED" OR status.state="SCHEDULED"',
    'CANCELED': 'status.state="CANCELLED"',
    'FAILURE': 'status.state="FAILED"',
    'SUCCESS': 'status.state="SUCCEEDED"',
}


def prepare_query_label_value(labels):
  """Converts the label strings to contain label-appropriate characters.

  Args:
    labels: A set of strings to be converted.

  Returns:
    A list of converted strings.
  """
  if not labels:
    return None
  return [job_model.convert_to_label_chars(label) for label in labels]


def label_filter(label_key, label_value):
  """Return a valid label filter for operations.list()."""
  return 'labels."{}" = "{}"'.format(label_key, label_value)


def create_time_filter(create_time, comparator):
  """Return a valid createTime filter for operations.list()."""
  return 'createTime {} "{}"'.format(comparator, create_time.isoformat())


# Generate command to create the directories for the dsub user environment
# pylint: disable=g-complex-comprehension
def make_runtime_dirs_command(script_dir: str, tmp_dir: str,
                              working_dir: str) -> str:
  return '\n'.join('mkdir -m 777 -p "%s" ' % dir
                   for dir in [script_dir, tmp_dir, working_dir])


# pylint: enable=g-complex-comprehension


# Action steps that interact with GCS need gsutil and Python.
# Use the 'slim' variant of the cloud-sdk image as it is much smaller.
CLOUD_SDK_IMAGE = 'gcr.io/google.com/cloudsdktool/cloud-sdk:294.0.0-slim'

# Name of the data disk
DATA_DISK_NAME = 'datadisk'

# Define a bash function for "echo" that includes timestamps
LOG_MSG_FN = textwrap.dedent("""\
  function get_datestamp() {
    date "+%Y-%m-%d %H:%M:%S"
  }

  function log_info() {
    echo "$(get_datestamp) INFO: $@"
  }

  function log_warning() {
    1>&2 echo "$(get_datestamp) WARNING: $@"
  }

  function log_error() {
    1>&2 echo "$(get_datestamp) ERROR: $@"
  }
""")

# Define a bash function for "gsutil cp" to be used by the logging,
# localization, and delocalization actions.
GSUTIL_CP_FN = textwrap.dedent("""\
  function gsutil_cp() {
    local src="${1}"
    local dst="${2}"
    local content_type="${3}"
    local user_project_name="${4}"

    local headers=""
    if [[ -n "${content_type}" ]]; then
      headers="-h Content-Type:${content_type}"
    fi

    local user_project_flag=""
    if [[ -n "${user_project_name}" ]]; then
      user_project_flag="-u ${user_project_name}"
    fi

    local attempt
    for ((attempt = 0; attempt < 4; attempt++)); do
      log_info "gsutil ${headers} ${user_project_flag} -mq cp \"${src}\" \"${dst}\""
      if gsutil ${headers} ${user_project_flag} -mq cp "${src}" "${dst}"; then
        return
      fi
      if (( attempt < 3 )); then
        log_warning "Sleeping 10s before the next attempt of failed gsutil command"
        log_warning "gsutil ${headers} ${user_project_flag} -mq cp \"${src}\" \"${dst}\""
        sleep 10s
      fi
    done

    log_error "gsutil ${headers} ${user_project_flag} -mq cp \"${src}\" \"${dst}\""
    exit 1
  }
""")

LOG_CP_FN = GSUTIL_CP_FN + textwrap.dedent("""\

  function log_cp() {
    local src="${1}"
    local dst="${2}"
    local tmp="${3}"
    local check_src="${4}"
    local user_project_name="${5}"

    if [[ "${check_src}" == "true" ]] && [[ ! -e "${src}" ]]; then
      return
    fi

    # Copy the log files to a local temporary location so that our "gsutil cp" is never
    # executed on a file that is changing.

    local tmp_path="${tmp}/$(basename ${src})"
    cp "${src}" "${tmp_path}"

    gsutil_cp "${tmp_path}" "${dst}" "text/plain" "${user_project_name}"
  }
""")

# Define a bash function for "gsutil rsync" to be used by the logging,
# localization, and delocalization actions.
GSUTIL_RSYNC_FN = textwrap.dedent("""\
  function gsutil_rsync() {
    local src="${1}"
    local dst="${2}"
    local user_project_name="${3}"

    local user_project_flag=""
    if [[ -n "${user_project_name}" ]]; then
      user_project_flag="-u ${user_project_name}"
    fi

    local attempt
    for ((attempt = 0; attempt < 4; attempt++)); do
      log_info "gsutil ${user_project_flag} -mq rsync -r \"${src}\" \"${dst}\""
      if gsutil ${user_project_flag} -mq rsync -r "${src}" "${dst}"; then
        return
      fi
      if (( attempt < 3 )); then
        log_warning "Sleeping 10s before the next attempt of failed gsutil command"
        log_warning "gsutil ${user_project_flag} -mq rsync -r \"${src}\" \"${dst}\""
        sleep 10s
      fi
    done

    log_error "gsutil ${user_project_flag} -mq rsync -r \"${src}\" \"${dst}\""
    exit 1
  }
""")

LOCALIZATION_LOOP = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  for ((i=0; i < INPUT_COUNT; i++)); do
    INPUT_VAR="INPUT_${i}"
    INPUT_RECURSIVE="INPUT_RECURSIVE_${i}"
    INPUT_SRC="INPUT_SRC_${i}"
    INPUT_DST="INPUT_DST_${i}"

    log_info "Localizing ${!INPUT_VAR}"
    if [[ "${!INPUT_RECURSIVE}" -eq "1" ]]; then
      gsutil_rsync "${!INPUT_SRC}" "${!INPUT_DST}" "${USER_PROJECT}"
    else
      gsutil_cp "${!INPUT_SRC}" "${!INPUT_DST}" "" "${USER_PROJECT}"
    fi
  done
""")

DELOCALIZATION_LOOP = textwrap.dedent("""\
  set -o errexit
  set -o nounset
  set -o pipefail

  for ((i=0; i < OUTPUT_COUNT; i++)); do
    OUTPUT_VAR="OUTPUT_${i}"
    OUTPUT_RECURSIVE="OUTPUT_RECURSIVE_${i}"
    OUTPUT_SRC="OUTPUT_SRC_${i}"
    OUTPUT_DST="OUTPUT_DST_${i}"

    log_info "Delocalizing ${!OUTPUT_VAR}"
    if [[ "${!OUTPUT_RECURSIVE}" -eq "1" ]]; then
      gsutil_rsync "${!OUTPUT_SRC}" "${!OUTPUT_DST}" "${USER_PROJECT}"
    else
      gsutil_cp "${!OUTPUT_SRC}" "${!OUTPUT_DST}" "" "${USER_PROJECT}"
    fi
  done
""")

LOCALIZATION_CMD = textwrap.dedent("""\
  {log_msg_fn}
  {recursive_cp_fn}
  {cp_fn}

  {cp_loop}
""")

# The user's script or command is made available to the container in
#   /mnt/data/script/<script-name>
#
# To get it there, it is passed in through the environment in the "prepare"
# action and "echo"-ed to a file.
#
# Google APIs use Docker environment files which do not support
# multi-line environment variables, so we encode the script using Python's
# repr() function and then decoded it using ast.literal_eval().
# This has the advantage over other encoding schemes (such as base64) of being
# user-readable in the LifeSciences "operation" or Batch "Job" object.
SCRIPT_VARNAME = '_SCRIPT_REPR'
META_YAML_VARNAME = '_META_YAML_REPR'

PYTHON_DECODE_SCRIPT = textwrap.dedent("""\
  import ast
  import sys

  sys.stdout.write(ast.literal_eval(sys.stdin.read()))
""")

MK_IO_DIRS = textwrap.dedent("""\
  for ((i=0; i < DIR_COUNT; i++)); do
    DIR_VAR="DIR_${i}"

    log_info "mkdir -m 777 -p \"${!DIR_VAR}\""
    mkdir -m 777 -p "${!DIR_VAR}"
  done
""")

PREPARE_CMD = textwrap.dedent("""\
  #!/bin/bash

  set -o errexit
  set -o nounset
  set -o pipefail

  {log_msg_fn}
  {mk_runtime_dirs}

  echo "${{{script_var}}}" \
    | python -c '{python_decode_script}' \
    > "{script_path}"
  chmod a+x "{script_path}"

  {mk_io_dirs}
""")

USER_CMD = textwrap.dedent("""\
  export TMPDIR="{tmp_dir}"
  cd {working_dir}

  "{user_script}"
""")


class GoogleJobProviderBase(base.JobProvider):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def _get_prepare_env(self, script, job_descriptor, inputs, outputs, mounts,
                       mount_point) -> Dict[str, str]:
    """Return a dict with variables for the 'prepare' action."""

    # Add the _SCRIPT_REPR with the repr(script) contents
    # Add the _META_YAML_REPR with the repr(meta) contents

    # Add variables for directories that need to be created, for example:
    # DIR_COUNT: 2
    # DIR_0: /mnt/data/input/gs/bucket/path1/
    # DIR_1: /mnt/data/output/gs/bucket/path2

    # List the directories in sorted order so that they are created in that
    # order. This is primarily to ensure that permissions are set as we create
    # each directory.
    # For example:
    #   mkdir -m 777 -p /root/first/second
    #   mkdir -m 777 -p /root/first
    # *may* not actually set 777 on /root/first

    docker_paths = sorted([
        var.docker_path if var.recursive else os.path.dirname(var.docker_path)
        for var in inputs | outputs | mounts
        if var.value
    ])

    env = {
        SCRIPT_VARNAME: repr(script.value),
        META_YAML_VARNAME: repr(job_descriptor.to_yaml()),
        'DIR_COUNT': str(len(docker_paths))
    }

    for idx, path in enumerate(docker_paths):
      env['DIR_{}'.format(idx)] = os.path.join(mount_point, path)

    return env

  def _get_localization_env(self, inputs, user_project,
                            mount_point) -> Dict[str, str]:
    """Return a dict with variables for the 'localization' action."""

    # Add variables for paths that need to be localized, for example:
    # INPUT_COUNT: 1
    # INPUT_0: MY_INPUT_FILE
    # INPUT_RECURSIVE_0: 0
    # INPUT_SRC_0: gs://mybucket/mypath/myfile
    # INPUT_DST_0: /mnt/data/inputs/mybucket/mypath/myfile

    non_empty_inputs = [var for var in inputs if var.value]
    env = {'INPUT_COUNT': str(len(non_empty_inputs))}

    for idx, var in enumerate(non_empty_inputs):
      env['INPUT_{}'.format(idx)] = var.name
      env['INPUT_RECURSIVE_{}'.format(idx)] = str(int(var.recursive))
      env['INPUT_SRC_{}'.format(idx)] = var.value

      # For wildcard paths, the destination must be a directory
      dst = os.path.join(mount_point, var.docker_path)
      path, filename = os.path.split(dst)
      if '*' in filename:
        dst = '{}/'.format(path)
      env['INPUT_DST_{}'.format(idx)] = dst

    env['USER_PROJECT'] = user_project

    return env

  def _get_delocalization_env(self, outputs, user_project,
                              mount_point) -> Dict[str, str]:
    """Return a dict with variables for the 'delocalization' action."""

    # Add variables for paths that need to be delocalized, for example:
    # OUTPUT_COUNT: 1
    # OUTPUT_0: MY_OUTPUT_FILE
    # OUTPUT_RECURSIVE_0: 0
    # OUTPUT_SRC_0: gs://mybucket/mypath/myfile
    # OUTPUT_DST_0: /mnt/data/outputs/mybucket/mypath/myfile

    non_empty_outputs = [var for var in outputs if var.value]
    env = {'OUTPUT_COUNT': str(len(non_empty_outputs))}

    for idx, var in enumerate(non_empty_outputs):
      env['OUTPUT_{}'.format(idx)] = var.name
      env['OUTPUT_RECURSIVE_{}'.format(idx)] = str(int(var.recursive))
      env['OUTPUT_SRC_{}'.format(idx)] = os.path.join(mount_point,
                                                      var.docker_path)

      # For wildcard paths, the destination must be a directory
      if '*' in var.uri.basename:
        dst = var.uri.path
      else:
        dst = var.uri
      env['OUTPUT_DST_{}'.format(idx)] = dst

    env['USER_PROJECT'] = user_project

    return env

  def _build_user_environment(self, envs, inputs, outputs, mounts,
                              mount_point) -> Dict[str, str]:
    """Returns a dictionary of for the user container environment."""
    envs = {env.name: env.value for env in envs}
    envs.update(
        providers_util.get_file_environment_variables(inputs, mount_point))
    envs.update(
        providers_util.get_file_environment_variables(outputs, mount_point))
    envs.update(
        providers_util.get_file_environment_variables(mounts, mount_point))
    return envs

  def prepare_job_metadata(self, script: str, job_name: str,
                           user_id: str) -> Dict[str, str]:
    return providers_util.prepare_job_metadata(script, job_name, user_id)

  def _get_label_filters(self, label_key, values):
    if not values or values == {'*'}:
      return None

    return [label_filter(label_key, v) for v in values]

  def _get_labels_filters(self, labels):
    if not labels:
      return None

    return [label_filter(l.name, l.value) for l in labels]

  def _get_status_filters(self, statuses):
    if not statuses or statuses == {'*'}:
      return None

    return [STATUS_FILTER_MAP[s] for s in statuses]

  def _get_user_id_filter_value(self, user_ids):
    if not user_ids or user_ids == {'*'}:
      return None

    return prepare_query_label_value(user_ids)

  def _get_create_time_filters(self, create_time_min, create_time_max):
    filters = []
    for create_time, comparator in [(create_time_min, '>='),
                                    (create_time_max, '<=')]:
      if not create_time:
        continue

      filters.append(create_time_filter(create_time, comparator))
    return filters

  def _build_query_filter(self,
                          statuses,
                          user_ids=None,
                          job_ids=None,
                          job_names=None,
                          task_ids=None,
                          task_attempts=None,
                          labels=None,
                          create_time_min=None,
                          create_time_max=None):
    # The Google APIs allows for building fairly elaborate filter
    # clauses. We can group (). We can AND, OR, and NOT.
    #
    # The first set of filters, labeled here as OR filters are elements
    # where more than one value cannot be true at the same time. For example,
    # an operation cannot have a status of both RUNNING and CANCELED.
    #
    # The second set of filters, labeled here as AND filters are elements
    # where more than one value can be true. For example,
    # an operation can have a label with key1=value2 AND key2=value2.

    # Translate the semantic requests into a google-specific filter.

    # 'OR' filtering arguments.
    status_filters = self._get_status_filters(statuses)
    user_id_filters = self._get_label_filters(
        'user-id', self._get_user_id_filter_value(user_ids))
    job_id_filters = self._get_label_filters('job-id', job_ids)
    job_name_filters = self._get_label_filters(
        'job-name', prepare_query_label_value(job_names))
    task_id_filters = self._get_label_filters('task-id', task_ids)
    task_attempt_filters = self._get_label_filters('task-attempt',
                                                   task_attempts)
    # 'AND' filtering arguments.
    label_filters = self._get_labels_filters(labels)
    create_time_filters = self._get_create_time_filters(create_time_min,
                                                        create_time_max)

    if job_id_filters and job_name_filters:
      raise ValueError(
          'Filtering by both job IDs and job names is not supported')

    # Now build up the full text filter.
    # OR all of the OR filter arguments together.
    # AND all of the AND arguments together.
    or_arguments = []
    for or_filters in [
        status_filters, user_id_filters, job_id_filters, job_name_filters,
        task_id_filters, task_attempt_filters
    ]:
      if or_filters:
        or_arguments.append('(' + ' OR '.join(or_filters) + ')')

    and_arguments = []
    for and_filters in [label_filters, create_time_filters]:
      if and_filters:
        and_arguments.append('(' + ' AND '.join(and_filters) + ')')

    # Now and all of these arguments together.
    return ' AND '.join(or_arguments + and_arguments)
