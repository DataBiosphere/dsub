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
import collections
from datetime import datetime
import itertools
import json
import os
import re
import socket
import string
import sys
import textwrap
import time
from . import base

from apiclient import errors
from apiclient.discovery import build
from dateutil.tz import tzlocal

from ..lib import param_util
from ..lib import providers_util
from oauth2client.client import GoogleCredentials
import pytz


_PROVIDER_NAME = 'google'

# Create file provider whitelist.
_SUPPORTED_FILE_PROVIDERS = frozenset([param_util.P_GCS])
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
    'mkdir --mode=777 -p "%s" ' % dir
    for dir in [SCRIPT_DIR, TMP_DIR, WORKING_DIR])

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
HTTP_ERROR_TOO_FREQUENT_POLLING = 429
TRANSIENT_HTTP_ERROR_CODES = set([500, 503, 504])
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


class _Label(collections.namedtuple('_Label', ['name', 'value'])):
  """Name/value label metadata for a pipeline.

  Attributes:
    name (str): the label name.
    value (str): the label value (optional).
  """
  __slots__ = ()

  def __new__(cls, name, value=None):
    param_util.LabelParam.validate_label(name, value)
    return super(_Label, cls).__new__(cls, name, value)

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


class _Api(object):

  @staticmethod
  def execute(api):
    # On success, return immediately.
    # On transient errors, retry after the retry_interval.
    # If we are polling too frequently, then back-off until success.

    retry_interval = 1

    while True:
      try:
        return api.execute()
      except errors.HttpError as e:
        if e.resp.status == HTTP_ERROR_TOO_FREQUENT_POLLING:
          retry_interval *= 2

          _print_error(
              ('Too frequent polling. Increasing retry interval to %d seconds.')
              % retry_interval)
        if e.resp.status not in TRANSIENT_HTTP_ERROR_CODES:
          raise e
      except socket.error as e:
        if e.errno not in TRANSIENT_SOCKET_ERROR_CODES:
          raise e

      if retry_interval > 0:
        time.sleep(retry_interval)


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
  def _build_pipeline_docker_command(cls, script_name, inputs, outputs):
    """Return a multi-line string containg the full pipeline docker command."""

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
          DATA_MOUNT_POINT, inputs, param_util.P_GCS)

    export_output_dirs = ''
    copy_output_dirs = ''
    if recursive_output_dirs:
      export_output_dirs = providers_util.build_recursive_gcs_delocalize_env(
          DATA_MOUNT_POINT, outputs)
      copy_output_dirs = providers_util.build_recursive_delocalize_command(
          DATA_MOUNT_POINT, outputs, param_util.P_GCS)

    mkdirs = '\n'.join([
        'mkdir -p {0}/{1}'.format(DATA_MOUNT_POINT, var.docker_path if
                                  var.recursive else
                                  os.path.dirname(var.docker_path))
        for var in outputs
    ])

    export_inputs_with_wildcards = ''
    inputs_with_wildcards = [
        var for var in inputs
        if not var.recursive and '*' in os.path.basename(var.docker_path)
    ]
    export_inputs_with_wildcards = '\n'.join([
        'export {0}="{1}/{2}"'.format(var.name, DATA_MOUNT_POINT,
                                      var.docker_path)
        for var in inputs_with_wildcards
    ])

    return DOCKER_COMMAND.format(
        mk_runtime_dirs=MK_RUNTIME_DIRS_COMMAND,
        script_path='%s/%s' % (SCRIPT_DIR, script_name),
        install_cloud_sdk=install_cloud_sdk,
        export_inputs_with_wildcards=export_inputs_with_wildcards,
        export_input_dirs=export_input_dirs,
        copy_input_dirs=copy_input_dirs,
        mk_output_dirs=mkdirs,
        export_output_dirs=export_output_dirs,
        tmpdir=TMP_DIR,
        working_dir=WORKING_DIR,
        copy_output_dirs=copy_output_dirs)

  @classmethod
  def build_pipeline(cls, project, min_cores, min_ram, disk_size,
                     boot_disk_size, preemptible, image, zones, script_name,
                     envs, inputs, outputs, pipeline_name):
    """Builds a pipeline configuration for execution.

    Args:
      project: string name of project.
      min_cores: int number of CPU cores required per job.
      min_ram: int GB of RAM required per job.
      disk_size: int GB of disk to attach under /mnt/data.
      boot_disk_size: int GB of disk for boot.
      preemptible: use a preemptible VM for the job
      image: string Docker image name in which to run.
      zones: list of zone names for jobs to be run at.
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
    # Format the docker command
    docker_command = cls._build_pipeline_docker_command(script_name, inputs,
                                                        outputs)

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

    input_envs = [{
        'name': SCRIPT_VARNAME
    }] + [{
        'name': env.name
    } for env in envs]

    input_files = [
        cls._build_pipeline_input_file_param(var.name, var.docker_path)
        for var in inputs if not var.recursive
    ]

    # Outputs are an array of file parameters
    output_files = [
        cls._build_pipeline_file_param(var.name, var.docker_path)
        for var in outputs if not var.recursive
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

                # Create a data disk that is attached to the VM and destroyed
                # when the pipeline terminates.
                'zones': _get_zones(zones),
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
  def build_pipeline_args(cls, project, script, job_data, preemptible,
                          logging_dir, scopes):
    """Builds pipeline args for execution.

    Args:
      project: string name of project.
      script: Body of the script to execute.
      job_data: dictionary of value for envs, inputs, and outputs for this
          pipeline instance.
      preemptible: use a preemptible VM for the job
      logging_dir: directory for job logging output.
      scopes: list of scope.

    Returns:
      A nested dictionary with one entry under the key pipelineArgs containing
      the pipeline arguments.
    """
    inputs = {}
    inputs.update({SCRIPT_VARNAME: script})
    inputs.update({var.name: var.value for var in job_data['envs']})
    inputs.update(
        {var.name: var.uri
         for var in job_data['inputs'] if not var.recursive})

    # Remove wildcard references for non-recursive output. When the pipelines
    # controller generates a delocalize call, it must point to a bare directory
    # for patterns. The output param OUTFILE=gs://bucket/path/*.bam should
    # delocalize with a call similar to:
    #   gsutil cp /mnt/data/output/gs/bucket/path/*.bam gs://bucket/path/
    outputs = {}
    for var in job_data['outputs']:
      if var.recursive:
        continue
      if '*' in var.uri.basename:
        outputs[var.name] = var.uri.path
      else:
        outputs[var.name] = var.uri

    labels = {}
    labels.update({
        label.name: label.value if label.value else ''
        for label in job_data['labels']
    })

    # pyformat: disable
    return {
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
                'gcsPath': logging_dir
            },
        }
    }
    # pyformat: enable

  @staticmethod
  def run_pipeline(service, pipeline):
    return _Api.execute(service.pipelines().run(body=pipeline))


class _Operations(object):
  """Utilty methods for querying and canceling pipeline operations."""

  @staticmethod
  def get_filter(project,
                 status=None,
                 user_id=None,
                 job_id=None,
                 job_name=None,
                 task_id=None,
                 create_time=None):
    """Return a filter string for operations.list()."""

    ops_filter = []
    ops_filter.append('projectId = %s' % project)
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

    if create_time:
      ops_filter.append('createTime >= %s' % create_time)

    return ' AND '.join(ops_filter)

  @staticmethod
  def list(service, ops_filter, max_ops=0):
    """Gets the list of operations for the specified filter.

    Args:
      service: Google Genomics API service object
      ops_filter: string filter of operations to return
      max_ops: maximum number of operations to return (0 indicates no maximum)

    Returns:
      A list of operations matching the filter criteria.
    """

    operations = []
    page_token = None
    page_size = None

    while not max_ops or len(operations) < max_ops:
      if max_ops:
        # If a maximum number of operations is requested, limit the requested
        # pageSize to the documented default (256) or less if we can.
        page_size = min(max_ops - len(operations), 256)

      api = service.operations().list(
          name='operations',
          filter=ops_filter,
          pageToken=page_token,
          pageSize=page_size)
      response = _Api.execute(api)

      ops = response['operations'] if 'operations' in response else None
      if ops:
        operations.extend(ops)

      # Exit if there are no more operations
      if 'nextPageToken' not in response or not response['nextPageToken']:
        break

      page_token = response['nextPageToken']

    if max_ops and len(operations) > max_ops:
      del operations[max_ops:]

    return operations

  @staticmethod
  def append_operation_error(error_messages, operation):
    if 'error' in operation:
      if 'task-id' in operation['metadata']['labels']:
        job_id = operation['metadata']['labels']['task-id']
      else:
        job_id = operation['metadata']['labels']['job-id']

      error_messages.append('Error in job %s - code %s: %s' %
                            (job_id, operation['error']['code'],
                             operation['error']['message']))

  @classmethod
  def _get_operation_input_field_values(cls, metadata, file_input):
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
    values = {name: vals_dict[name] for name in names if name in vals_dict}

    return values

  @classmethod
  def get_operation_field(cls, operation, field, default=None):
    """Returns a value from the operation for a specific set of field names.

    Args:
      operation: an operation object returned from operations.list()
      field: a dsub-specific job metadata key
      default: default value to return if field does not exist or is empty.

    Returns:
      A text string for the field or a list for 'inputs'.

    Raises:
      ValueError: if the field label is not supported by the operation
    """

    metadata = operation.get('metadata')

    if field == 'internal-id':
      value = operation['name']
    elif field == 'job-name':
      value = metadata['labels'].get('job-name')
    elif field == 'job-id':
      value = metadata['labels'].get('job-id')
    elif field == 'task-id':
      value = metadata['labels'].get('task-id')
    elif field == 'user-id':
      value = metadata['labels'].get('user-id')
    elif field == 'job-status':
      value = metadata['job-status']
    elif field == 'envs':
      value = cls._get_operation_input_field_values(metadata, False)
    elif field == 'labels':
      return {}
    elif field == 'inputs':
      value = cls._get_operation_input_field_values(metadata, True)
    elif field == 'outputs':
      value = metadata['request']['pipelineArgs']['outputs']
    elif field == 'create-time':
      value = cls._localize_datestamp(metadata['createTime'])
    elif field == 'end-time':
      if 'endTime' in metadata:
        value = cls._localize_datestamp(metadata['endTime'])
      else:
        value = None
    elif field == 'status':
      value = cls.operation_status(operation)
    elif field in ['status-message', 'status-detail']:
      status, last_update = cls.operation_status_message(operation)
      value = status
    elif field == 'last-update':
      status, last_update = cls.operation_status_message(operation)
      value = last_update
    else:
      raise ValueError('Unsupported display field: "%s"' % field)

    return value if value else default

  @classmethod
  def _get_operation_full_job_id(cls, op):
    """Returns the job-id or job-id.task-id for the operation."""
    job_id = cls.get_operation_field(op, 'job-id')
    task_id = cls.get_operation_field(op, 'task-id')
    if task_id:
      return '%s.%s' % (job_id, task_id)
    else:
      return job_id

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
      ops_by_name[op['name']] = op
      batch.add(
          service.operations().cancel(name=op['name'], body={}),
          request_id=op['name'])

    # Cancel the operations
    batch.execute()

    # Iterate through the canceled and failed lists to build our return lists
    canceled_ops = [ops_by_name[op['name']] for op in canceled]
    error_messages = [
        "Error canceling '%s': %s" %
        (cls._get_operation_full_job_id(ops_by_name[op['name']]), op['msg'])
        for op in failed
    ]

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

  @classmethod
  def _localize_datestamp(cls, datestamp):
    """Converts a datestamp from RFC3339 UTC to local time.

    Args:
      datestamp: a datetime value in RFC3339 UTC "Zulu" format

    Returns:
      A datestamp in local time and up to seconds, or the original string if it
      cannot be properly parsed.
    """

    # The timestamp from the Google Operations are all in RFC3339 format, but
    # they are sometimes formatted to nanoseconds and sometimes only seconds.
    # Parse both:
    # * 2016-11-14T23:04:55Z
    # * 2016-11-14T23:05:56.010429380Z
    # And any sub-second precision in-between.

    m = re.match(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).*Z',
                 datestamp)
    if not m:
      return datestamp

    # Create a UTC datestamp from parsed components
    g = [int(val) for val in m.groups()]
    dt = datetime(g[0], g[1], g[2], g[3], g[4], g[5], tzinfo=pytz.utc)
    return dt.astimezone(tzlocal()).strftime('%Y-%m-%d %H:%M:%S')

  @classmethod
  def operation_status(cls, operation):
    """Returns the status of this operation.

    ie. RUNNING, SUCCESS, CANCELED or FAILURE.

    Args:
      operation: Operation

    Returns:
      A printable status string
    """
    if not operation['done']:
      return 'RUNNING'
    if 'error' not in operation:
      return 'SUCCESS'
    if operation['error'].get('code', 0) == 1:
      return 'CANCELED'
    return 'FAILURE'

  @classmethod
  def operation_status_message(cls, operation):
    """Returns the most relevant status string and last updated date string.

    This string is meant for display only.

    Args:
      operation: Operation

    Returns:
      A printable status string and date string.
    """
    metadata = operation['metadata']
    if not operation['done']:
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

      if 'error' in operation:
        # Shorten message if it's too long.
        msg = operation['error']['message']
      else:
        msg = 'Success'

    return (msg, cls._localize_datestamp(ds))


class GoogleJobProvider(base.JobProvider):
  """Interface to dsub and related tools for managing Google cloud jobs."""

  def __init__(self, verbose, dry_run, project, zones=None, credentials=None):
    self._verbose = verbose
    self._dry_run = dry_run

    self._project = project
    self._zones = zones

    self._service = self._setup_service(credentials)

  @staticmethod
  def _setup_service(credentials=None):
    """Configures genomics API client.

    Args:
      credentials: credentials to be used for the gcloud API calls.

    Returns:
      A configured Google Genomics API client with appropriate credentials.
    """
    if not credentials:
      credentials = GoogleCredentials.get_application_default()
    return build('genomics', 'v1alpha2', credentials=credentials)

  def prepare_job_metadata(self, script, job_name, user_id):
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
                             datetime.now().strftime('%y%m%d-%H%M%S-%f')[:16])

    return {
        'pipeline-name': pipeline_name,
        'job-name': job_name_value,
        'job-id': job_id,
        'user-id': user_id
    }

  def _build_pipeline_labels(self, job_metadata, task_id):
    labels = [
        _Label(name, job_metadata[name])
        for name in ['job-name', 'job-id', 'user-id']
    ]

    if task_id:
      labels.append(_Label('task-id', 'task-%d' % task_id))

    return labels

  def _build_pipeline_request(self, job_resources, job_metadata, job_data):
    """Returns a Pipeline objects for the job."""

    script = job_metadata['script']
    job_data['labels'] = self._build_pipeline_labels(job_metadata,
                                                     job_data.get('task_id'))

    # Build the ephemeralPipeline for this job.
    # The ephemeralPipeline definition changes for each job because file
    # parameters localCopy.path changes based on the remote_uri.
    pipeline = _Pipelines.build_pipeline(
        project=self._project,
        min_cores=job_resources.min_cores,
        min_ram=job_resources.min_ram,
        disk_size=job_resources.disk_size,
        boot_disk_size=job_resources.boot_disk_size,
        preemptible=job_resources.preemptible,
        image=job_resources.image,
        zones=job_resources.zones,
        script_name=script.name,
        envs=job_data['envs'],
        inputs=job_data['inputs'],
        outputs=job_data['outputs'],
        pipeline_name=job_metadata['pipeline-name'])

    # Build the pipelineArgs for this job.
    pipeline.update(
        _Pipelines.build_pipeline_args(
            self._project, script.value, job_data, job_resources.preemptible,
            job_resources.logging.uri, job_resources.scopes))

    return pipeline

  def _submit_pipeline(self, request):
    operation = _Pipelines.run_pipeline(self._service, request)
    if self._verbose:
      print 'Launched operation %s' % operation['name']

    return self.get_task_field(operation, 'task-id')

  def submit_job(self, job_resources, job_metadata, all_task_data):
    """Submit the job (or tasks) to be executed.

    Args:
      job_resources: resource parameters required by each job.
      job_metadata: job parameters such as job-id, user-id, script
      all_task_data: list of task arguments

    Returns:
      A dictionary containing the 'user-id', 'job-id', and 'task-id' list.
      For jobs that are not task array jobs, the task-id list should be empty.

    Raises:
      ValueError: if job resources or task data contain illegal values.
    """
    # Validate task data and resources.
    param_util.validate_submit_args_or_fail(
        job_resources,
        all_task_data,
        provider_name=_PROVIDER_NAME,
        input_providers=_SUPPORTED_INPUT_PROVIDERS,
        output_providers=_SUPPORTED_OUTPUT_PROVIDERS,
        logging_providers=_SUPPORTED_LOGGING_PROVIDERS)
    # Prepare and submit jobs.
    launched_tasks = []
    requests = []
    for job_data in all_task_data:
      request = self._build_pipeline_request(job_resources, job_metadata,
                                             job_data)

      if self._dry_run:
        requests.append(request)
      else:
        task = self._submit_pipeline(request)
        if task:
          launched_tasks.append(task)

    # If this is a dry-run, emit all the pipeline request objects
    if self._dry_run:
      print json.dumps(requests, indent=2, sort_keys=True)

    return {
        'job-id': job_metadata['job-id'],
        'user-id': job_metadata['user-id'],
        'task-id': launched_tasks
    }

  def lookup_job_tasks(self,
                       status_list,
                       user_list=None,
                       job_list=None,
                       job_name_list=None,
                       task_list=None,
                       create_time=None,
                       max_tasks=0):
    """Return a list of operations based on the input criteria.

    If any of the filters are empty or ["*"], then no filtering is performed on
    that field. Filtering by both a job id list and job name list is
    unsupported.

    Args:
      status_list: ['*'], or a list of job status strings to return. Valid
        status strings are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_list: a list of ids for the user(s) who launched the job.
      job_list: a list of job ids to return.
      job_name_list: a list of job names to return.
      task_list: a list of specific tasks within the specified job(s) to return.
      create_time: a UTC value for earliest create time for a task.
      max_tasks: the maximum number of job tasks to return or 0 for no limit.

    Raises:
      ValueError: if both a job id list and a job name list are provided

    Returns:
      A list of Genomics API Operations objects.
    """

    # Server-side, we can filter on status, job_id, user_id, task_id, but there
    # is no OR filter (only AND), and so we can't handle lists server side.
    # In practice we don't expect combinations of user lists and job lists.
    # For now, do the most brain-dead thing and if we find a common use-case
    # that performs poorly, we can re-evaluate.

    status_list = status_list if status_list else ['*']
    user_list = user_list if user_list else ['*']
    job_list = job_list if job_list else ['*']
    job_name_list = job_name_list if job_name_list else ['*']
    task_list = task_list if task_list else ['*']

    if set(job_list) != set(['*']) and set(job_name_list) != set(['*']):
      raise ValueError(
          'Filtering by both job IDs and job names is not supported')

    tasks = []
    for status, job_id, job_name, user_id, task_id in itertools.product(
        status_list, job_list, job_name_list, user_list, task_list):
      ops_filter = _Operations.get_filter(
          self._project,
          status=status,
          user_id=user_id,
          job_id=job_id,
          job_name=job_name,
          task_id=task_id,
          create_time=create_time)

      ops = _Operations.list(self._service, ops_filter, max_tasks)
      for o in ops:
        o['metadata']['job-status'] = _Operations.operation_status(o)

      if ops:
        tasks.extend(ops)

      if max_tasks and len(tasks) > max_tasks:
        del tasks[max_tasks:]
        return tasks

    return tasks

  def delete_jobs(self, user_list, job_list, task_list, create_time=None):
    """Kills the operations associated with the specified job or job.task.

    Args:
      user_list: List of user ids who "own" the job(s) to cancel.
      job_list: List of job_ids to cancel.
      task_list: List of task-ids to cancel.
      create_time: a UTC value for earliest create time for a task.

    Returns:
      A list of tasks canceled and a list of error messages.
    """
    # Look up the job(s)
    tasks = self.lookup_job_tasks(
        ['RUNNING'],
        user_list=user_list,
        job_list=job_list,
        task_list=task_list,
        create_time=create_time)

    print 'Found %d tasks to delete.' % len(tasks)

    return _Operations.cancel(self._service, tasks)

  def get_task_field(self, task, field, default=None):
    return _Operations.get_operation_field(task, field, default)

  def get_task_status_message(self, task):
    """Returns the most relevant status string and last updated date string.

    This string is meant for display only.

    Args:
      task: the operation for which to get status.

    Returns:
      A printable status string and date string.
    """
    return _Operations.operation_status_message(task)

  def get_tasks_completion_messages(self, tasks):
    error_messages = []
    for task in tasks:
      _Operations.append_operation_error(error_messages, task)

    return error_messages


if __name__ == '__main__':
  pass
