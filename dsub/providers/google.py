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
"""Provider for running jobs on Google Cloud Platform.

This module implements job creation, listing, and canceling using the
Google Genomics Pipelines and Operations APIs.
"""

# pylint: disable=g-tzinfo-datetime
from datetime import datetime
import itertools
import json
import os
import re
import socket
import string
import sys
import textwrap
from . import base
from .._dsub_version import DSUB_VERSION

import apiclient.discovery
import apiclient.errors

# TODO(b/68858502) Fix the use of relative imports throughout this library
from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import providers_util
from ..lib import sorting_util
from oauth2client.client import GoogleCredentials
from oauth2client.client import HttpAccessTokenRefreshError
import pytz
import retrying

_PROVIDER_NAME = 'google'

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([job_model.P_GCS])
_SUPPORTED_LOGGING_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_INPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS
_SUPPORTED_OUTPUT_PROVIDERS = _SUPPORTED_FILE_PROVIDERS

# Environment variable name for the script body
SCRIPT_VARNAME = '_SCRIPT'

# Mount point for the data disk on the VM and in the Docker container
DATA_MOUNT_POINT = '/mnt/data'

# Special dsub directories within the Docker container
#
# Attempt to keep the dsub runtime environment sane by being very prescriptive.
# Assume a single disk for everything that needs to be written by the dsub
# runtime environment or the user.
#
# Backends like the Google Pipelines API, allow for the user to set both
# a boot-disk-size and a disk-size. But the boot-disk-size is not something
# that users should have to worry about, so don't put anything extra there.
#
# Put everything meaningful on the data disk:
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

SCRIPT_DIR = '%s/script' % DATA_MOUNT_POINT
TMP_DIR = '%s/tmp' % DATA_MOUNT_POINT
WORKING_DIR = '%s/workingdir' % DATA_MOUNT_POINT

MK_RUNTIME_DIRS_COMMAND = '\n'.join(
    'mkdir -m 777 -p "%s" ' % dir for dir in [SCRIPT_DIR, TMP_DIR, WORKING_DIR])

DOCKER_COMMAND = textwrap.dedent("""\
  set -o errexit
  set -o nounset

  # Create runtime directories
  {mk_runtime_dirs}

  # Write the script to a file and make it executable
  echo "${{_SCRIPT}}" > "{script_path}"
  chmod u+x "{script_path}"

  # Install gsutil if there are recursive copies to do
  {install_cloud_sdk}

  # Set environment variables for inputs with wildcards
  {export_inputs_with_wildcards}

  # Set environment variables for recursive input directories
  {export_input_dirs}

  # Recursive copy input directories
  {copy_input_dirs}

  # Create the output directories
  {mk_output_dirs}

  # Set environment variables for recursive output directories
  {export_output_dirs}

  # Set empty environment variables
  {export_empty_envs}

  # Set TMPDIR
  export TMPDIR="{tmpdir}"

  # DEPRECATED: do not use DATA_ROOT
  export DATA_ROOT=/mnt/data

  # Set the working directory
  cd "{working_dir}"

  # Run the user script
  "{script_path}"

  # Recursive copy output directories
  {copy_output_dirs}
""")

# If an output directory is marked as "recursive", then dsub takes over the
# responsibilities of de-localizing that output directory.
#
# If the docker image already has gsutil in it, then we just use it.
# For large numbers of pipelines that utilize the recursive output feature,
# including Cloud SDK in the docker image is generally preferred.
#
# When one is just getting started with their pipeline, adding Cloud SDK
# installation in their docker image should not be a requirement.
INSTALL_CLOUD_SDK = textwrap.dedent("""\
  if ! type gsutil; then
    apt-get update
    apt-get --yes install ca-certificates gcc gnupg2 python-dev python-setuptools
    easy_install -U pip
    pip install -U crcmod

    apt-get --yes install lsb-release
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
    echo "deb http://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" >> /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update && apt-get --yes install curl
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
    apt-get update && apt-get --yes install google-cloud-sdk
  fi
""")

# Transient errors for the Google APIs should not cause them to fail.
# There are a set of HTTP and socket errors which we automatically retry.
#  429: too frequent polling
#  50x: backend error
TRANSIENT_HTTP_ERROR_CODES = set([429, 500, 503, 504])

# Socket error 104 (connection reset) should also be retried
TRANSIENT_SOCKET_ERROR_CODES = set([104])

# When attempting to cancel an operation that is already completed
# (succeeded, failed, or canceled), the response will include:
# "error": {
#    "code": 400,
#    "status": "FAILED_PRECONDITION",
# }
FAILED_PRECONDITION_CODE = 400
FAILED_PRECONDITION_STATUS = 'FAILED_PRECONDITION'

# List of Compute Engine zones, which enables simple wildcard expansion.
# We could look this up dynamically, but new zones come online
# infrequently enough, this is easy to keep up with.
# Also - the Pipelines API may one day directly support zone wildcards.
#
# To refresh this list:
#   gcloud compute zones list --format='value(name)' \
#     | sort | awk '{ printf "    '\''%s'\'',\n", $1 }'
_ZONES = [
    'asia-east1-a',
    'asia-east1-b',
    'asia-east1-c',
    'asia-northeast1-a',
    'asia-northeast1-b',
    'asia-northeast1-c',
    'asia-south1-a',
    'asia-south1-b',
    'asia-south1-c',
    'asia-southeast1-a',
    'asia-southeast1-b',
    'australia-southeast1-a',
    'australia-southeast1-b',
    'australia-southeast1-c',
    'europe-west1-b',
    'europe-west1-c',
    'europe-west1-d',
    'europe-west2-a',
    'europe-west2-b',
    'europe-west2-c',
    'europe-west3-a',
    'europe-west3-b',
    'europe-west3-c',
    'europe-west4-b',
    'europe-west4-c',
    'northamerica-northeast1-a',
    'northamerica-northeast1-b',
    'northamerica-northeast1-c',
    'southamerica-east1-a',
    'southamerica-east1-b',
    'southamerica-east1-c',
    'us-central1-a',
    'us-central1-b',
    'us-central1-c',
    'us-central1-f',
    'us-east1-b',
    'us-east1-c',
    'us-east1-d',
    'us-east4-a',
    'us-east4-b',
    'us-east4-c',
    'us-west1-a',
    'us-west1-b',
    'us-west1-c',
]


def _get_zones(input_list):
  """Returns a list of zones based on any wildcard input.

  This function is intended to provide an easy method for producing a list
  of desired zones for a pipeline to run in.

  The Pipelines API default zone list is "any zone". The problem with
  "any zone" is that it can lead to incurring Cloud Storage egress charges
  if the GCE zone selected is in a different region than the GCS bucket.
  See https://cloud.google.com/storage/pricing#network-egress.

  A user with a multi-region US bucket would want to pipelines to run in
  a "us-*" zone.
  A user with a regional bucket in US would want to restrict pipelines to
  run in a zone in that region.

  Rarely does the specific zone matter for a pipeline.

  This function allows for a simple short-hand such as:
     [ "us-*" ]
     [ "us-central1-*" ]
  These examples will expand out to the full list of US and us-central1 zones
  respectively.

  Args:
    input_list: list of zone names/patterns

  Returns:
    A list of zones, with any wildcard zone specifications expanded.
  """

  output_list = []

  for zone in input_list:
    if zone.endswith('*'):
      prefix = zone[:-1]
      output_list.extend([z for z in _ZONES if z.startswith(prefix)])
    else:
      output_list.append(zone)

  return output_list


def _print_error(msg):
  """Utility routine to emit messages to stderr."""
  print >> sys.stderr, msg


class _Label(job_model.LabelParam):
  """Name/value label metadata for a pipeline.

  Attributes:
    name (str): the label name.
    value (str): the label value (optional).
  """
  _allow_reserved_keys = True
  __slots__ = ()

  @staticmethod
  def convert_to_label_chars(s):
    """Turn the specified name and value into a valid Google label."""

    # We want the results to be user-friendly, not just functional.
    # So we can't base-64 encode it.
    #   * If upper-case: lower-case it
    #   * If the char is not a standard letter or digit. make it a dash
    accepted_characters = string.ascii_lowercase + string.digits + '-'

    def label_char_transform(char):
      if char in accepted_characters:
        return char
      if char in string.ascii_uppercase:
        return char.lower()
      return '-'

    return ''.join(label_char_transform(c) for c in s)


def _retry_api_check(exception):
  """Return True if we should retry. False otherwise.

  Args:
    exception: An exception to test for transience.

  Returns:
    True if we should retry. False otherwise.
  """
  _print_error('Exception %s: %s' % (type(exception).__name__, str(exception)))

  if isinstance(exception, apiclient.errors.HttpError):
    if exception.resp.status in TRANSIENT_HTTP_ERROR_CODES:
      return True

  if isinstance(exception, socket.error):
    if exception.errno in TRANSIENT_SOCKET_ERROR_CODES:
      return True

  if isinstance(exception, HttpAccessTokenRefreshError):
    return True

  return False


class _Api(object):

  # Exponential backoff retrying API execution.
  # Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
  @staticmethod
  @retrying.retry(
      stop_max_attempt_number=23,
      retry_on_exception=_retry_api_check,
      wait_exponential_multiplier=1000,
      wait_exponential_max=64000)
  def execute(api):
    return api.execute()


class _Pipelines(object):
  """Utilty methods for creating pipeline operations."""

  @classmethod
  def _build_pipeline_input_file_param(cls, var_name, docker_path):
    """Return a dict object representing a pipeline input argument."""

    # If the filename contains a wildcard, then the target Docker path must
    # be a directory in order to ensure consistency whether the source pattern
    # contains 1 or multiple files.
    #
    # In that case, we set the docker_path to explicitly have a trailing slash
    # (for the Pipelines API "gsutil cp" handling, and then override the
    # associated var_name environment variable in the generated Docker command.

    path, filename = os.path.split(docker_path)
    if '*' in filename:
      return cls._build_pipeline_file_param(var_name, path + '/')
    else:
      return cls._build_pipeline_file_param(var_name, docker_path)

  @classmethod
  def _build_pipeline_file_param(cls, var_name, docker_path):
    """Return a dict object representing a pipeline input or output argument."""
    return {
        'name': var_name,
        'localCopy': {
            'path': docker_path,
            'disk': 'datadisk'
        }
    }

  @classmethod
  def _build_pipeline_docker_command(cls, script_name, inputs, outputs, envs):
    """Return a multi-line string of the full pipeline docker command."""

    # We upload the user script as an environment argument
    # and write it to SCRIPT_DIR (preserving its local file name).
    #
    # The docker_command:
    # * writes the script body to a file
    # * installs gcloud if there are recursive copies to do
    # * sets environment variables for inputs with wildcards
    # * sets environment variables for recursive input directories
    # * recursively copies input directories
    # * creates output directories
    # * sets environment variables for recursive output directories
    # * sets the DATA_ROOT environment variable to /mnt/data
    # * sets the working directory to ${DATA_ROOT}
    # * executes the user script
    # * recursively copies output directories
    recursive_input_dirs = [var for var in inputs if var.recursive]
    recursive_output_dirs = [var for var in outputs if var.recursive]

    install_cloud_sdk = ''
    if recursive_input_dirs or recursive_output_dirs:
      install_cloud_sdk = INSTALL_CLOUD_SDK

    export_input_dirs = ''
    copy_input_dirs = ''
    if recursive_input_dirs:
      export_input_dirs = providers_util.build_recursive_localize_env(
          DATA_MOUNT_POINT, inputs)
      copy_input_dirs = providers_util.build_recursive_localize_command(
          DATA_MOUNT_POINT, inputs, job_model.P_GCS)

    export_output_dirs = ''
    copy_output_dirs = ''
    if recursive_output_dirs:
      export_output_dirs = providers_util.build_recursive_gcs_delocalize_env(
          DATA_MOUNT_POINT, outputs)
      copy_output_dirs = providers_util.build_recursive_delocalize_command(
          DATA_MOUNT_POINT, outputs, job_model.P_GCS)

    docker_paths = [
        var.docker_path if var.recursive else os.path.dirname(var.docker_path)
        for var in outputs
    ]

    mkdirs = '\n'.join([
        'mkdir -p {0}/{1}'.format(DATA_MOUNT_POINT, path)
        for path in docker_paths
    ])

    inputs_with_wildcards = [
        var for var in inputs
        if not var.recursive and '*' in os.path.basename(var.docker_path)
    ]
    export_inputs_with_wildcards = '\n'.join([
        'export {0}="{1}/{2}"'.format(var.name, DATA_MOUNT_POINT,
                                      var.docker_path)
        for var in inputs_with_wildcards
    ])

    export_empty_envs = '\n'.join(
        ['export {0}=""'.format(var.name) for var in envs if not var.value])

    return DOCKER_COMMAND.format(
        mk_runtime_dirs=MK_RUNTIME_DIRS_COMMAND,
        script_path='%s/%s' % (SCRIPT_DIR, script_name),
        install_cloud_sdk=install_cloud_sdk,
        export_inputs_with_wildcards=export_inputs_with_wildcards,
        export_input_dirs=export_input_dirs,
        copy_input_dirs=copy_input_dirs,
        mk_output_dirs=mkdirs,
        export_output_dirs=export_output_dirs,
        export_empty_envs=export_empty_envs,
        tmpdir=TMP_DIR,
        working_dir=WORKING_DIR,
        copy_output_dirs=copy_output_dirs)

  @classmethod
  def build_pipeline(cls, project, zones, min_cores, min_ram, disk_size,
                     boot_disk_size, preemptible, accelerator_type,
                     accelerator_count, image, script_name, envs, inputs,
                     outputs, pipeline_name):
    """Builds a pipeline configuration for execution.

    Args:
      project: string name of project.
      zones: list of zone names for jobs to be run at.
      min_cores: int number of CPU cores required per job.
      min_ram: int GB of RAM required per job.
      disk_size: int GB of disk to attach under /mnt/data.
      boot_disk_size: int GB of disk for boot.
      preemptible: use a preemptible VM for the job
      accelerator_type: string GCE defined accelerator type.
      accelerator_count: int number of accelerators of the specified type to
        attach.
      image: string Docker image name in which to run.
      script_name: file name of the script to run.
      envs: list of EnvParam objects specifying environment variables to set
        within each job.
      inputs: list of FileParam objects specifying input variables to set
        within each job.
      outputs: list of FileParam objects specifying output variables to set
        within each job.
      pipeline_name: string name of pipeline.

    Returns:
      A nested dictionary with one entry under the key emphemeralPipeline
      containing the pipeline configuration.
    """
    if min_cores is None:
      min_cores = job_model.DEFAULT_MIN_CORES
    if min_ram is None:
      min_ram = job_model.DEFAULT_MIN_RAM
    if disk_size is None:
      disk_size = job_model.DEFAULT_DISK_SIZE
    if boot_disk_size is None:
      boot_disk_size = job_model.DEFAULT_BOOT_DISK_SIZE
    if preemptible is None:
      preemptible = job_model.DEFAULT_PREEMPTIBLE

    # Format the docker command
    docker_command = cls._build_pipeline_docker_command(script_name, inputs,
                                                        outputs, envs)

    # Pipelines inputParameters can be both simple name/value pairs which get
    # set as environment variables, as well as input file paths which the
    # Pipelines controller will automatically localize to the Pipeline VM.

    # In the ephemeralPipeline object, the inputParameters are only defined;
    # the values are passed in the pipelineArgs.

    # Pipelines outputParameters are only output file paths, which the
    # Pipelines controller can automatically de-localize after the docker
    # command completes.

    # The Pipelines API does not support recursive copy of file parameters,
    # so it is implemented within the dsub-generated pipeline.
    # Any inputs or outputs marked as "recursive" are completely omitted here;
    # their environment variables will be set in the docker command, and
    # recursive copy code will be generated there as well.

    # The Pipelines API does not accept empty environment variables. Set them to
    # empty in DOCKER_COMMAND instead.
    input_envs = [{
        'name': SCRIPT_VARNAME
    }] + [{
        'name': env.name
    } for env in envs if env.value]

    input_files = [
        cls._build_pipeline_input_file_param(var.name, var.docker_path)
        for var in inputs
        if not var.recursive
    ]

    # Outputs are an array of file parameters
    output_files = [
        cls._build_pipeline_file_param(var.name, var.docker_path)
        for var in outputs
        if not var.recursive
    ]

    # The ephemeralPipeline provides the template for the pipeline.
    # pyformat: disable
    return {
        'ephemeralPipeline': {
            'projectId': project,
            'name': pipeline_name,

            # Define the resources needed for this pipeline.
            'resources': {
                'minimumCpuCores': min_cores,
                'minimumRamGb': min_ram,
                'bootDiskSizeGb': boot_disk_size,
                'preemptible': preemptible,
                'zones': _get_zones(zones),
                'acceleratorType': accelerator_type,
                'acceleratorCount': accelerator_count,

                # Create a data disk that is attached to the VM and destroyed
                # when the pipeline terminates.
                'disks': [{
                    'name': 'datadisk',
                    'autoDelete': True,
                    'sizeGb': disk_size,
                    'mountPoint': DATA_MOUNT_POINT,
                }],
            },

            'inputParameters': input_envs + input_files,
            'outputParameters': output_files,

            'docker': {
                'imageName': image,
                'cmd': docker_command,
            }
        }
    }
    # pyformat: enable

  @classmethod
  def build_pipeline_args(cls, project, script, job_params, task_params,
                          preemptible, logging_uri, scopes, keep_alive):
    """Builds pipeline args for execution.

    Args:
      project: string name of project.
      script: Body of the script to execute.
      job_params: dictionary of values for labels, envs, inputs, and outputs
          for this job.
      task_params: dictionary of values for labels, envs, inputs, and outputs
          for this task.
      preemptible: use a preemptible VM for the job
      logging_uri: path for job logging output.
      scopes: list of scope.
      keep_alive: Seconds to keep VM alive on failure

    Returns:
      A nested dictionary with one entry under the key pipelineArgs containing
      the pipeline arguments.
    """
    # For the Pipelines API, envs and file inputs are all "inputs".
    inputs = {}
    inputs.update({SCRIPT_VARNAME: script})
    inputs.update({
        var.name: var.value
        for var in job_params['envs'] | task_params['envs']
        if var.value
    })
    inputs.update({
        var.name: var.uri
        for var in job_params['inputs'] | task_params['inputs']
        if not var.recursive
    })

    # Remove wildcard references for non-recursive output. When the pipelines
    # controller generates a delocalize call, it must point to a bare directory
    # for patterns. The output param OUTFILE=gs://bucket/path/*.bam should
    # delocalize with a call similar to:
    #   gsutil cp /mnt/data/output/gs/bucket/path/*.bam gs://bucket/path/
    outputs = {}
    for var in job_params['outputs'] | task_params['outputs']:
      if var.recursive:
        continue
      if '*' in var.uri.basename:
        outputs[var.name] = var.uri.path
      else:
        outputs[var.name] = var.uri

    labels = {}
    labels.update({
        label.name: label.value if label.value else ''
        for label in job_params['labels'] | task_params['labels']
    })

    # pyformat: disable
    args = {
        'pipelineArgs': {
            'projectId': project,
            'resources': {
                'preemptible': preemptible,
            },
            'inputs': inputs,
            'outputs': outputs,
            'labels': labels,
            'serviceAccount': {
                'email': 'default',
                'scopes': scopes,
            },
            # Pass the user-specified GCS destination for pipeline logging.
            'logging': {
                'gcsPath': logging_uri
            },
        }
    }
    # pyformat: enable

    if keep_alive:
      args['pipelineArgs'][
          'keep_vm_alive_on_failure_duration'] = '%ss' % keep_alive

    return args

  @staticmethod
  def run_pipeline(service, pipeline):
    return _Api.execute(service.pipelines().run(body=pipeline))


class _Operations(object):
  """Utilty methods for querying and canceling pipeline operations."""

  @staticmethod
  def _datetime_to_utc_int(date):
    """Convert the integer UTC time value into a local datetime."""
    if date is None:
      return None

    # Convert localized datetime to a UTC integer
    epoch = dsub_util.replace_timezone(datetime.utcfromtimestamp(0), pytz.utc)
    return (date - epoch).total_seconds()

  @staticmethod
  def get_filter(project,
                 status=None,
                 user_id=None,
                 job_id=None,
                 job_name=None,
                 labels=None,
                 task_id=None,
                 create_time_min=None,
                 create_time_max=None):
    """Return a filter string for operations.list()."""

    ops_filter = ['projectId = %s' % project]
    if status and status != '*':
      ops_filter.append('status = %s' % status)

    if user_id != '*':
      ops_filter.append('labels.user-id = %s' % user_id)
    if job_id != '*':
      ops_filter.append('labels.job-id = %s' % job_id)
    if job_name != '*':
      ops_filter.append('labels.job-name = %s' % job_name)
    if task_id != '*':
      ops_filter.append('labels.task-id = %s' % task_id)

    # Even though labels are nominally 'arbitrary strings' they are trusted
    # since param_util restricts the character set.
    if labels:
      for l in labels:
        ops_filter.append('labels.%s = %s' % (l.name, l.value))

    epoch = dsub_util.replace_timezone(datetime.utcfromtimestamp(0), pytz.utc)
    if create_time_min:
      create_time_min_utc_int = (create_time_min - epoch).total_seconds()
      ops_filter.append('createTime >= %d' % create_time_min_utc_int)
    if create_time_max:
      create_time_max_utc_int = (create_time_max - epoch).total_seconds()
      ops_filter.append('createTime <= %d' % create_time_max_utc_int)

    return ' AND '.join(ops_filter)

  @classmethod
  def get_operation_type(cls, op):
    return op.get('metadata', {}).get('request', {}).get('@type')

  @classmethod
  def get_operation_label(cls, op, name):
    return op.get('metadata', {}).get('labels', {}).get(name)

  @classmethod
  def is_pipelines_operation(cls, op):
    """Check that an operation is a genomics pipeline run.

    An operation is a Genomics Pipeline run if the request metadata's @type
    is "type.googleapis.com/google.genomics.v1alpha2.RunPipelineRequest.

    Args:
      op: a pipelines operation.

    Returns:
      Boolean, true if the operation is a RunPipelineRequest.
    """
    return cls.get_operation_type(
        op) == 'type.googleapis.com/google.genomics.v1alpha2.RunPipelineRequest'

  @classmethod
  def is_dsub_operation(cls, op):
    """Determine if a pipelines operation is a dsub request.

    We don't have a rigorous way to identify an operation as being submitted
    by dsub. Our best option is to check for certain fields that have always
    been part of dsub operations.

    - labels: job-id, job-name, and user-id have always existed
    - envs: _SCRIPT has always existed.

    In order to keep a simple heuristic this test only uses labels.
    Args:
      op: a pipelines operation.

    Returns:
      Boolean, true if the pipeline run was generated by dsub.
    """
    if not cls.is_pipelines_operation(op):
      return False

    for name in ['job-id', 'job-name', 'user-id']:
      if not cls.get_operation_label(op, name):
        return False

    return True

  @classmethod
  def list(cls, service, ops_filter, page_size=0):
    """Gets the list of operations for the specified filter.

    Args:
      service: Google Genomics API service object
      ops_filter: string filter of operations to return
      page_size: the number of operations to requested on each list operation to
        the pipelines API (if 0 or None, the API default is used)

    Yields:
      Operations matching the filter criteria.
    """

    page_token = None
    more_operations = True
    documented_default_page_size = 256
    documented_max_page_size = 2048

    if not page_size:
      page_size = documented_default_page_size
    page_size = min(page_size, documented_max_page_size)

    while more_operations:
      api = service.operations().list(
          name='operations',
          filter=ops_filter,
          pageToken=page_token,
          pageSize=page_size)
      response = _Api.execute(api)

      ops = response.get('operations', [])
      for op in ops:
        if cls.is_dsub_operation(op):
          yield GoogleOperation(op)

      page_token = response.get('nextPageToken')
      more_operations = bool(page_token)

  @classmethod
  def _cancel_batch(cls, service, ops):
    """Cancel a batch of operations.

    Args:
      service: Google Genomics API service object.
      ops: A list of operations to cancel.

    Returns:
      A list of operations canceled and a list of error messages.
    """

    # We define an inline callback which will populate a list of
    # successfully canceled operations as well as a list of operations
    # which were not successfully canceled.

    canceled = []
    failed = []

    def handle_cancel(request_id, response, exception):
      """Callback for the cancel response."""
      del response  # unused

      if exception:
        # We don't generally expect any failures here, except possibly trying
        # to cancel an operation that is already canceled or finished.
        #
        # If the operation is already finished, provide a clearer message than
        # "error 400: Bad Request".

        msg = 'error %s: %s' % (exception.resp.status, exception.resp.reason)
        if exception.resp.status == FAILED_PRECONDITION_CODE:
          detail = json.loads(exception.content)
          status = detail.get('error', {}).get('status')
          if status == FAILED_PRECONDITION_STATUS:
            msg = 'Not running'

        failed.append({'name': request_id, 'msg': msg})
      else:
        canceled.append({'name': request_id})

      return

    # Set up the batch object
    batch = service.new_batch_http_request(callback=handle_cancel)

    # The callback gets a "request_id" which is the operation name.
    # Build a dict such that after the callback, we can lookup the operation
    # objects by name
    ops_by_name = {}
    for op in ops:
      op_name = op.get_field('internal-id')
      ops_by_name[op_name] = op
      batch.add(
          service.operations().cancel(name=op_name, body={}),
          request_id=op_name)

    # Cancel the operations
    batch.execute()

    # Iterate through the canceled and failed lists to build our return lists
    canceled_ops = [ops_by_name[cancel['name']] for cancel in canceled]
    error_messages = []
    for fail in failed:
      message = "Error canceling '%s': %s"
      op = ops_by_name[fail['name']]
      message %= (op.get_operation_full_job_id(), fail['msg'])
      error_messages.append(message)

    return canceled_ops, error_messages

  @classmethod
  def cancel(cls, service, ops):
    """Cancel operations.

    Args:
      service: Google Genomics API service object.
      ops: A list of operations to cancel.

    Returns:
      A list of operations canceled and a list of error messages.
    """

    # Canceling many operations one-by-one can be slow.
    # The Pipelines API doesn't directly support a list of operations to cancel,
    # but the requests can be performed in batch.

    canceled_ops = []
    error_messages = []

    max_batch = 256
    total_ops = len(ops)
    for first_op in range(0, total_ops, max_batch):
      batch_canceled, batch_messages = cls._cancel_batch(
          service, ops[first_op:first_op + max_batch])
      canceled_ops.extend(batch_canceled)
      error_messages.extend(batch_messages)

    return canceled_ops, error_messages


class GoogleJobProvider(base.JobProvider):
  """Interface to dsub and related tools for managing Google cloud jobs."""

  def __init__(self, verbose, dry_run, project, zones=None, credentials=None):
    self._verbose = verbose
    self._dry_run = dry_run

    self._project = project
    self._zones = zones

    self._service = self._setup_service(credentials)

  # Exponential backoff retrying API discovery.
  # Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
  @classmethod
  @retrying.retry(
      stop_max_attempt_number=23,
      retry_on_exception=_retry_api_check,
      wait_exponential_multiplier=1000,
      wait_exponential_max=64000)
  def _do_setup_service(cls, credentials):
    return apiclient.discovery.build(
        'genomics', 'v1alpha2', credentials=credentials)

  @classmethod
  def _setup_service(cls, credentials=None):
    """Configures genomics API client.

    Args:
      credentials: credentials to be used for the gcloud API calls.

    Returns:
      A configured Google Genomics API client with appropriate credentials.
    """
    if not credentials:
      credentials = GoogleCredentials.get_application_default()
    return cls._do_setup_service(credentials)

  def prepare_job_metadata(self, script, job_name, user_id, create_time):
    """Returns a dictionary of metadata fields for the job."""

    # The name of the pipeline gets set into the ephemeralPipeline.name as-is.
    # The default name of the pipeline is the script name
    # The name of the job is derived from the job_name and gets set as a
    # 'job-name' label (and so the value must be normalized).
    if job_name:
      pipeline_name = job_name
      job_name_value = _Label.convert_to_label_chars(job_name)
    else:
      pipeline_name = os.path.basename(script)
      job_name_value = _Label.convert_to_label_chars(
          pipeline_name.split('.', 1)[0])

    # The user-id will get set as a label
    user_id = _Label.convert_to_label_chars(user_id)

    # Now build the job-id. We want the job-id to be expressive while also
    # having a low-likelihood of collisions.
    #
    # For expressiveness, we:
    # * use the job name (truncated at 10 characters).
    # * insert the user-id
    # * add a datetime value
    # To have a high likelihood of uniqueness, the datetime value is out to
    # hundredths of a second.
    #
    # The full job-id is:
    #   <job-name>--<user-id>--<timestamp>
    job_id = '%s--%s--%s' % (job_name_value[:10], user_id,
                             create_time.strftime('%y%m%d-%H%M%S-%f')[:16])

    # Standard version is MAJOR.MINOR(.PATCH). This will convert the version
    # string to "vMAJOR-MINOR(-PATCH)". Example; "0.1.0" -> "v0-1-0".
    version = _Label.convert_to_label_chars('v%s' % DSUB_VERSION)
    return {
        'pipeline-name': pipeline_name,
        'job-name': job_name_value,
        'job-id': job_id,
        'user-id': user_id,
        'dsub-version': version,
    }

  def _build_pipeline_labels(self, job_metadata, task_metadata):
    labels = {
        _Label(name, job_metadata[name])
        for name in ['job-name', 'job-id', 'user-id', 'dsub-version']
    }

    if task_metadata.get('task-id') is not None:
      labels.add(_Label('task-id', 'task-%d' % task_metadata.get('task-id')))

    return labels

  def _build_pipeline_request(self, task_view):
    """Returns a Pipeline objects for the job."""
    job_metadata = task_view.job_metadata
    job_params = task_view.job_params
    job_resources = task_view.job_resources
    task_metadata = task_view.task_descriptors[0].task_metadata
    task_params = task_view.task_descriptors[0].task_params
    task_resources = task_view.task_descriptors[0].task_resources

    script = task_view.job_metadata['script']
    task_params['labels'] |= self._build_pipeline_labels(
        job_metadata, task_metadata)

    # Build the ephemeralPipeline for this job.
    # The ephemeralPipeline definition changes for each job because file
    # parameters localCopy.path changes based on the remote_uri.
    pipeline = _Pipelines.build_pipeline(
        project=self._project,
        zones=job_resources.zones,
        min_cores=job_resources.min_cores,
        min_ram=job_resources.min_ram,
        disk_size=job_resources.disk_size,
        boot_disk_size=job_resources.boot_disk_size,
        preemptible=job_resources.preemptible,
        accelerator_type=job_resources.accelerator_type,
        accelerator_count=job_resources.accelerator_count,
        image=job_resources.image,
        script_name=script.name,
        envs=job_params['envs'] | task_params['envs'],
        inputs=job_params['inputs'] | task_params['inputs'],
        outputs=job_params['outputs'] | task_params['outputs'],
        pipeline_name=job_metadata['pipeline-name'])

    # Build the pipelineArgs for this job.
    logging_uri = task_resources.logging_path.uri
    scopes = job_resources.scopes or job_model.DEFAULT_SCOPES
    pipeline.update(
        _Pipelines.build_pipeline_args(self._project, script.value, job_params,
                                       task_params, job_resources.preemptible,
                                       logging_uri, scopes,
                                       job_resources.keep_alive))

    return pipeline

  def _submit_pipeline(self, request):
    operation = _Pipelines.run_pipeline(self._service, request)
    if self._verbose:
      print 'Launched operation %s' % operation['name']

    return GoogleOperation(operation).get_field('task-id')

  def submit_job(self, job_descriptor, skip_if_output_present):
    """Submit the job (or tasks) to be executed.

    Args:
      job_descriptor: all parameters needed to launch all job tasks
      skip_if_output_present: (boolean) if true, skip tasks whose output
        is present (see --skip flag for more explanation).

    Returns:
      A dictionary containing the 'user-id', 'job-id', and 'task-id' list.
      For jobs that are not task array jobs, the task-id list should be empty.

    Raises:
      ValueError: if job resources or task data contain illegal values.
    """
    # Validate task data and resources.
    param_util.validate_submit_args_or_fail(
        job_descriptor,
        provider_name=_PROVIDER_NAME,
        input_providers=_SUPPORTED_INPUT_PROVIDERS,
        output_providers=_SUPPORTED_OUTPUT_PROVIDERS,
        logging_providers=_SUPPORTED_LOGGING_PROVIDERS)

    # Prepare and submit jobs.
    launched_tasks = []
    requests = []
    for task_view in job_model.task_view_generator(job_descriptor):

      job_params = task_view.job_params
      task_params = task_view.task_descriptors[0].task_params

      outputs = job_params['outputs'] | task_params['outputs']
      if skip_if_output_present:
        # check whether the output's already there
        if dsub_util.outputs_are_present(outputs):
          print 'Skipping task because its outputs are present'
          continue

      request = self._build_pipeline_request(task_view)

      if self._dry_run:
        requests.append(request)
      else:
        task_id = self._submit_pipeline(request)
        launched_tasks.append(task_id)

    # If this is a dry-run, emit all the pipeline request objects
    if self._dry_run:
      print json.dumps(requests, indent=2, sort_keys=True)

    if not requests and not launched_tasks:
      return {'job-id': dsub_util.NO_JOB}

    return {
        'job-id': job_descriptor.job_metadata['job-id'],
        'user-id': job_descriptor.job_metadata['user-id'],
        'task-id': [task_id for task_id in launched_tasks if task_id],
    }

  def lookup_job_tasks(self,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0,
                       page_size=0):
    """Yields operations based on the input criteria.

    If any of the filters are empty or {'*'}, then no filtering is performed on
    that field. Filtering by both a job id list and job name list is
    unsupported.

    Args:
      statuses: {'*'}, or a list of job status strings to return. Valid
        status strings are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_ids: a list of ids for the user(s) who launched the job.
      job_ids: a list of job ids to return.
      job_names: a list of job names to return.
      task_ids: a list of specific tasks within the specified job(s) to return.
      labels: a list of LabelParam with user-added labels. All labels must
              match the task being fetched.
      create_time_min: a timezone-aware datetime value for the earliest create
                       time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
                       create time of a task, inclusive.
      max_tasks: the maximum number of job tasks to return or 0 for no limit.
      page_size: the page size to use for each query to the pipelins API.

    Raises:
      ValueError: if both a job id list and a job name list are provided

    Yeilds:
      Genomics API Operations objects.
    """

    # Server-side, we can filter on status, job_id, user_id, task_id, but there
    # is no OR filter (only AND), so we can't handle lists server side.
    # Therefore we construct a set of queries for each possible combination of
    # these criteria.
    statuses = statuses if statuses else {'*'}
    user_ids = user_ids if user_ids else {'*'}
    job_ids = job_ids if job_ids else {'*'}
    job_names = job_names if job_names else {'*'}
    task_ids = task_ids if task_ids else {'*'}

    # The task-id label value of "task-n" instead of just "n" is a hold-over
    # from early label value character restrictions.
    # Accept both forms, "task-n" and "n", for lookups by task-id.
    task_ids = {'task-{}'.format(t) if t.isdigit() else t for t in task_ids}

    if job_ids != {'*'} and job_names != {'*'}:
      raise ValueError(
          'Filtering by both job IDs and job names is not supported')

    # AND filter rule arguments.
    labels = labels if labels else set()

    # The results of all these queries need to be sorted by create-time
    # (descending). To accomplish this, each query stream (already sorted by
    # create-time) is added to a SortedGeneratorIterator which is a wrapper
    # around a PriorityQueue of iterators (sorted by each stream's newest task's
    # create-time). A sorted list can then be built by stepping through this
    # iterator and adding tasks until none are left or we hit max_tasks.

    now = datetime.now()

    def _desc_date_sort_key(t):
      return now - dsub_util.replace_timezone(t.get_field('create-time'), None)

    query_queue = sorting_util.SortedGeneratorIterator(key=_desc_date_sort_key)
    for status, job_id, job_name, user_id, task_id in itertools.product(
        statuses, job_ids, job_names, user_ids, task_ids):
      ops_filter = _Operations.get_filter(
          self._project,
          status=status,
          user_id=user_id,
          job_id=job_id,
          job_name=job_name,
          labels=labels,
          task_id=task_id,
          create_time_min=create_time_min,
          create_time_max=create_time_max)

      # The pipelines API returns operations sorted by create-time date. We can
      # use this sorting guarantee to merge-sort the streams together and only
      # retrieve more tasks as needed.
      stream = _Operations.list(self._service, ops_filter, page_size=page_size)
      query_queue.add_generator(stream)

    tasks_yielded = 0
    for task in query_queue:
      yield task
      tasks_yielded += 1
      if 0 < max_tasks < tasks_yielded:
        break

  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    """Kills the operations associated with the specified job or job.task.

    Args:
      user_ids: List of user ids who "own" the job(s) to cancel.
      job_ids: List of job_ids to cancel.
      task_ids: List of task-ids to cancel.
      labels: List of LabelParam, each must match the job(s) to be canceled.
      create_time_min: a timezone-aware datetime value for the earliest create
                       time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
                       create time of a task, inclusive.

    Returns:
      A list of tasks canceled and a list of error messages.
    """
    # Look up the job(s)
    tasks = list(
        self.lookup_job_tasks(
            {'RUNNING'},
            user_ids=user_ids,
            job_ids=job_ids,
            task_ids=task_ids,
            labels=labels,
            create_time_min=create_time_min,
            create_time_max=create_time_max))

    print 'Found %d tasks to delete.' % len(tasks)

    return _Operations.cancel(self._service, tasks)

  def get_tasks_completion_messages(self, tasks):
    completion_messages = []
    for task in tasks:
      errmsg = task.error_message()
      completion_messages.append(errmsg)
    return completion_messages


class GoogleOperation(base.Task):
  """Task wrapper around a Pipelines API operation object."""

  def __init__(self, operation_data):
    self._op = operation_data
    # Sanity check for operation_status().
    unused_status = self.operation_status()

  def raw_task_data(self):
    return self._op

  def get_field(self, field, default=None):
    """Returns a value from the operation for a specific set of field names.

    Args:
      field: a dsub-specific job metadata key
      default: default value to return if field does not exist or is empty.

    Returns:
      A text string for the field or a list for 'inputs'.

    Raises:
      ValueError: if the field label is not supported by the operation
    """

    metadata = self._op.get('metadata')

    value = None
    if field == 'internal-id':
      value = self._op['name']
    elif field == 'job-id':
      value = metadata['labels'].get('job-id')
    elif field == 'job-name':
      value = metadata['labels'].get('job-name')
    elif field == 'task-id':
      value = metadata['labels'].get('task-id')
    elif field == 'user-id':
      value = metadata['labels'].get('user-id')
    elif field == 'dsub-version':
      value = metadata['labels'].get('dsub-version')
    elif field == 'task-status':
      value = self.operation_status()
    elif field == 'logging':
      value = metadata['request']['pipelineArgs']['logging']['gcsPath']
    elif field == 'envs':
      value = self._get_operation_input_field_values(metadata, False)
    elif field == 'labels':
      # Reserved labels are filtered from dsub task output.
      value = {
          k: v
          for k, v in metadata['labels'].items()
          if k not in job_model.RESERVED_LABELS
      }
    elif field == 'inputs':
      value = self._get_operation_input_field_values(metadata, True)
    elif field == 'outputs':
      value = self._get_operation_output_field_values(metadata)
    elif field == 'create-time':
      value = self._parse_datestamp(metadata['createTime'])
    elif field == 'start-time':
      # Look through the events list for all "start" events (only one expected).
      start_events = [
          e for e in metadata.get('events', []) if e['description'] == 'start'
      ]
      # Get the startTime from the last "start" event.
      if start_events:
        value = self._parse_datestamp(start_events[-1]['startTime'])
    elif field == 'end-time':
      if 'endTime' in metadata:
        value = self._parse_datestamp(metadata['endTime'])
    elif field == 'status':
      value = self.operation_status()
    elif field in ['status-message', 'status-detail']:
      status, last_update = self.operation_status_message()
      value = status
    elif field == 'last-update':
      status, last_update = self.operation_status_message()
      value = last_update
    else:
      raise ValueError('Unsupported field: "%s"' % field)

    return value if value else default

  def operation_status(self):
    """Returns the status of this operation.

    ie. RUNNING, SUCCESS, CANCELED or FAILURE.

    Returns:
      A printable status string
    """
    if not self._op['done']:
      return 'RUNNING'
    if 'error' not in self._op:
      return 'SUCCESS'
    if self._op['error'].get('code', 0) == 1:
      return 'CANCELED'
    return 'FAILURE'

  def operation_status_message(self):
    """Returns the most relevant status string and last updated date string.

    This string is meant for display only.

    Returns:
      A printable status string and date string.
    """
    metadata = self._op['metadata']
    if not self._op['done']:
      if 'events' in metadata and metadata['events']:
        # Get the last event
        last_event = metadata['events'][-1]

        msg = last_event['description']
        ds = last_event['startTime']
      else:
        msg = 'Pending'
        ds = metadata['createTime']
    else:
      ds = metadata['endTime']

      if 'error' in self._op:
        # Shorten message if it's too long.
        msg = self._op['error']['message']
      else:
        msg = 'Success'

    return (msg, self._parse_datestamp(ds))

  def get_operation_full_job_id(self):
    """Returns the job-id or job-id.task-id for the operation."""
    job_id = self.get_field('job-id')
    task_id = self.get_field('task-id')
    if task_id:
      return '%s.%s' % (job_id, task_id)
    else:
      return job_id

  @staticmethod
  def _parse_datestamp(datestamp):
    """Converts a datestamp from RFC3339 UTC to a datetime.

    Args:
      datestamp: a datetime string in RFC3339 UTC "Zulu" format

    Returns:
      A datetime.
    """

    # The timestamp from the Google Operations are all in RFC3339 format, but
    # they are sometimes formatted to nanoseconds and sometimes only seconds.
    # Parse both:
    # * 2016-11-14T23:04:55Z
    # * 2016-11-14T23:05:56.010429380Z
    # And any sub-second precision in-between.

    m = re.match(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).*Z',
                 datestamp)

    # It would be unexpected to get a different date format back from Google.
    # If we raise an exception here, we can break people completely.
    # Instead, let's just return None and people can report that some dates
    # are not showing up.
    if not m:
      return None

    # Create a UTC datestamp from parsed components
    g = [int(val) for val in m.groups()]
    return datetime(g[0], g[1], g[2], g[3], g[4], g[5], tzinfo=pytz.utc)

  def _get_operation_input_field_values(self, metadata, file_input):
    """Returns a dictionary of envs or file inputs for an operation.

    Args:
      metadata: operation metadata field
      file_input: True to return a dict of file inputs, False to return envs.

    Returns:
      A dictionary of input field name value pairs
    """

    # To determine input parameter type, we iterate through the
    # pipeline inputParameters.
    # The values come from the pipelineArgs inputs.
    input_args = metadata['request']['ephemeralPipeline']['inputParameters']
    vals_dict = metadata['request']['pipelineArgs']['inputs']

    # Get the names for files or envs
    names = [
        arg['name'] for arg in input_args if ('localCopy' in arg) == file_input
    ]

    # Build the return dict
    return {name: vals_dict[name] for name in names if name in vals_dict}

  def _get_operation_output_field_values(self, metadata):
    # When outputs with wildcards are constructed, the "value" has the
    # basename removed (see build_pipeline_args).
    # We can recover the basename from the docker path.
    output_params = metadata['request']['ephemeralPipeline']['outputParameters']
    output_args = metadata['request']['pipelineArgs']['outputs']

    outputs = {}
    for key, value in output_args.items():
      if value.endswith('/'):
        param = next(p for p in output_params if p['name'] == key)
        docker_path = param['localCopy']['path']
        value = os.path.join(value, os.path.basename(docker_path))
      outputs[key] = value

    return outputs

  def error_message(self):
    """Returns an error message if the operation failed for any reason.

    Failure as defined here means; ended for any reason other than 'success'.
    This means that a successful cancelation will also create an error message
    here.

    Returns:
      string, string will be empty if job did not error.
    """
    if 'error' in self._op:
      if 'task-id' in self._op['metadata']['labels']:
        job_id = self._op['metadata']['labels']['task-id']
      else:
        job_id = self._op['metadata']['labels']['job-id']
      return 'Error in job %s - code %s: %s' % (job_id,
                                                self._op['error']['code'],
                                                self._op['error']['message'])
    else:
      return ''


if __name__ == '__main__':
  pass
