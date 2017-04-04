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
import json
import os
import re
import socket
import string
import sys
import textwrap
import time

from apiclient import errors
from apiclient.discovery import build
from dateutil.tz import tzlocal

from oauth2client.client import GoogleCredentials
import pytz

DATA_MOUNT_POINT = '/mnt/data'  # Mount point for the data disk on the VM

SCRIPT_VARNAME = '_SCRIPT'  # Environment variable name for the script body
SCRIPT_PATH = '/src'  # Location inside docker container for the user script

DOCKER_COMMAND = textwrap.dedent("""\
  set -o errexit
  set -o nounset

  readonly SCRIPT_PATH="%s/%s"

  # Write the script to a file and make it executable
  mkdir -p $(dirname "${SCRIPT_PATH}")
  echo "${%s}" > "${SCRIPT_PATH}"
  chmod u+x "${SCRIPT_PATH}"

  # Install gsutil if there are recursive copies to do
  %s

  # Set environment variables for recursive input directories
  %s

  # Recursive copy input directories
  %s

  # Create the output directories
  %s

  # Set environment variables for recursive output directories
  %s

  # Make the DATA_ROOT directory available to the user script
  export DATA_ROOT=/mnt/data
  cd "${DATA_ROOT}"

  # Run the user script
  "${SCRIPT_PATH}"

  # Recursive copy output directories
  %s
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
    apt-get --yes install gcc python-dev python-setuptools ca-certificates
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
#     | sort | awk '{ printf "'\''%s'\'',\n", $1 }'
_ZONES = [
    'asia-east1-a',
    'asia-east1-b',
    'asia-east1-c',
    'asia-northeast1-a',
    'asia-northeast1-b',
    'asia-northeast1-c',
    'europe-west1-b',
    'europe-west1-c',
    'europe-west1-d',
    'us-central1-a',
    'us-central1-b',
    'us-central1-c',
    'us-central1-f',
    'us-east1-b',
    'us-east1-c',
    'us-east1-d',
    'us-west1-a',
    'us-west1-b',
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
    cls.validate_name(name)
    cls.validate_value(value)
    return super(_Label, cls).__new__(cls, name, value)

  @staticmethod
  def validate_name(name):
    """Check that the name conforms to Google label restrictions."""

    # The name must conform to RFC1035 (domain names):
    #   * must be 1-63 characters long
    #   * match the regular expression [a-z]([-a-z0-9]*[a-z0-9])?

    if len(name) < 1 or len(name) > 63:
      raise ValueError('Label name must be 1-63 characters long: %s' % name)
    if not re.match(r'^[a-z]([-a-z0-9]*[a-z0-9])?$', name):
      raise ValueError('Invalid name for label: %s' % name)

  @staticmethod
  def validate_value(value, label_type='label'):
    """Check that the value conforms to Google label restrictions."""

    # The value can be empty.
    # If not empty, must conform to RFC1035 (domain names).
    #   * match the regular expression [a-z]([-a-z0-9]*[a-z0-9])?

    if value and not re.match(r'^[a-z]([-a-z0-9]*[a-z0-9])?$', value):
      raise ValueError('Invalid value for %s: %s' % (label_type, value))

  @staticmethod
  def convert_to_label_chars(s):
    """Turn the specified name and value into a valid Google label."""

    # We want the results to be user-friendly, not just functional.
    # So we don't base-64 encode or any nonsense like that.

    # Algorithm:
    #   * For each character
    #     * If upper-case: lower-case it
    #     * If underscore period, etc. make it a dash
    # pyformat: disable
    lst = [c if c in string.ascii_lowercase
           else c if c in string.digits
           else c if c == '-'
           else c.lower() if c in string.ascii_uppercase
           else '-' for c in s]
    # pyformat: enable

    return ''.join(lst)


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
    # and write it to /src (preserving its local file name).
    #
    # The docker_command:
    # * writes the script body to a file
    # * installs gcloud if there are recursive copies to do
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
      export_input_dirs = '\n'.join([
          'export {0}={1}/{2}'.format(var.name, DATA_MOUNT_POINT,
                                      var.docker_path.rstrip('/'))
          for var in recursive_input_dirs
      ])

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
          """).format(var.remote_uri, DATA_MOUNT_POINT, var.docker_path)
          for var in recursive_input_dirs
      ])

    export_output_dirs = ''
    copy_output_dirs = ''
    if recursive_output_dirs:
      export_output_dirs = '\n'.join([
          'export {0}={1}/{2}'.format(var.name, DATA_MOUNT_POINT,
                                      var.docker_path.rstrip('/'))
          for var in recursive_output_dirs
      ])

      copy_output_dirs = '\n'.join([
          textwrap.dedent("""
          for ((i = 0; i < 3; i++)); do
            if gsutil -m rsync -r {0}/{1} {2}; then
              break
            elif ((i == 2)); then
              2>&1 echo "Recursive de-localization failed."
              exit 1
            fi
          done
          """).format(DATA_MOUNT_POINT, var.docker_path, var.remote_uri)
          for var in recursive_output_dirs
      ])

    mkdirs = '\n'.join([
        'mkdir -p {0}/{1}'.format(DATA_MOUNT_POINT, var.docker_path if
                                  var.recursive else
                                  os.path.dirname(var.docker_path))
        for var in outputs
    ])

    return DOCKER_COMMAND % (SCRIPT_PATH, script_name, SCRIPT_VARNAME,
                             install_cloud_sdk, export_input_dirs,
                             copy_input_dirs, mkdirs, export_output_dirs,
                             copy_output_dirs)

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
        cls._build_pipeline_file_param(var.name, var.docker_path)
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
    inputs.update({
        var.name: var.remote_uri
        for var in job_data['inputs'] if not var.recursive
    })

    outputs = {}
    outputs.update({
        var.name: var.remote_uri
        for var in job_data['outputs'] if not var.recursive
    })

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
  def get_filter(project, status=None, user_id=None, job_id=None, task_id=None):
    """Return a filter string for operations.list()."""

    ops_filter = []
    ops_filter.append('projectId = %s' % project)
    if status and status != '*':
      ops_filter.append('status = %s' % status)

    if user_id != '*':
      ops_filter.append('labels.user-id = %s' % user_id)
    if job_id != '*':
      ops_filter.append('labels.job-id = %s' % job_id)
    if task_id != '*':
      ops_filter.append('labels.task-id = %s' % task_id)

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
  def get_operation_field(cls, operation, field):
    """Returns a value from the operation for a specific set of field names.

    Args:
      operation: an operation object returned from operations.list()
      field: a dsub-specific job metadata key

    Returns:
      A text string for the field or a list for 'inputs'.

    Raises:
      ValueError: if the field label is not supported by the operation
    """

    if field == 'internal-id':
      return operation['name']

    metadata = operation['metadata']

    if field == 'job-name':
      return metadata['labels'].get('job-name')
    elif field == 'job-id':
      return metadata['labels'].get('job-id')
    elif field == 'task-id':
      return metadata['labels'].get('task-id')
    elif field == 'user-id':
      return metadata['labels'].get('user-id')
    elif field == 'job-status':
      return metadata['job-status']
    elif field == 'inputs':
      return metadata['request']['pipelineArgs']['inputs']
    elif field == 'create-time':
      return cls._localize_datestamp(metadata['createTime'])
    elif field == 'end-time':
      if 'endTime' in metadata:
        return cls._localize_datestamp(metadata['endTime'])
    else:
      raise ValueError('Unsupported display field: %s' % field)

  @classmethod
  def cancel(cls, service, ops):
    """Cancel the operation."""

    canceled_ops = []
    error_messages = []
    for op in ops:
      try:
        _Api.execute(service.operations().cancel(name=op['name'], body={}))
        canceled_ops.append(op)
      except errors.HttpError as e:
        # If the operation is already finished, quietly continue
        if e.resp.status == FAILED_PRECONDITION_CODE:
          detail = json.loads(e.content)
          if 'error' in detail and 'status' in detail['error']['status']:
            if detail['error']['status'] == FAILED_PRECONDITION_STATUS:
              continue

        job_id = cls.get_operation_field(op, 'job-id')
        task_id = cls.get_operation_field(op, 'task-id')
        if task_id:
          res_id = '%s.%s' % (job_id, task_id)
        else:
          res_id = job_id

        error_messages.append('Error canceling job %s - code %s: %s' %
                              (res_id, e.resp.status, e.resp.reason))

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


class GoogleJobProvider(object):
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

  def get_job_metadata(self, script, pipeline_name, user_id, is_table):
    """Returns a dictionary of metadata fields for the job."""

    # The name of the pipeline gets set into the ephemeralPipeline.name as-is.
    # The default name of the pipeline is the script name
    # The name of the job is derived from the pipeline_name and gets set as a
    # 'job-name' label (and so the value must be normalized).
    if pipeline_name:
      job_name = _Label.convert_to_label_chars(pipeline_name)
    else:
      pipeline_name = os.path.basename(script)
      job_name = _Label.convert_to_label_chars(pipeline_name.split('.', 1)[0])

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
    job_id = '%s--%s--%s' % (job_name[:10], user_id,
                             datetime.now().strftime('%y%m%d-%H%M%S-%f')[:16])

    return {
        'is_table': is_table,
        'pipeline-name': pipeline_name,
        'job-name': job_name,
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

  def _build_pipeline_request(self, job_resources, job_metadata, job_data,
                              task_id):
    """Returns a Pipeline objects for the job."""

    script = job_metadata['script']
    job_data['labels'] = self._build_pipeline_labels(job_metadata, task_id)

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
            job_resources.logging, job_resources.scopes))

    return pipeline

  def _submit_pipeline(self, request):
    operation = _Pipelines.run_pipeline(self._service, request)
    if self._verbose:
      print 'Launched operation %s' % operation['name']

    return self.get_job_field(operation, 'task-id')

  def submit_job(self, job_resources, job_metadata, all_job_data):
    """Submit the job (or tasks) to be executed.

    Args:
      job_resources: resource parameters required by each job.
      job_metadata: job parameters such as job-id, user-id, script
      all_job_data: list of job (or task) arguments

    Returns:
      A dictionary containing the 'job-id' and if there are tasks, a list
      of the task ids under the key 'task-id'.
    """

    launched_tasks = []
    requests = []
    for job_index, job_data in enumerate(all_job_data):
      request = self._build_pipeline_request(job_resources, job_metadata,
                                             job_data, job_index + 1 if
                                             job_metadata['is_table'] else None)

      if self._dry_run:
        requests.append(request)
      else:
        task = self._submit_pipeline(request)
        launched_tasks.append(task)

    # If this is a dry-run, emit all the pipeline request objects
    if self._dry_run:
      print json.dumps(requests, indent=2, sort_keys=True)

    launched_job = {
        'job-id': job_metadata['job-id'],
        'user-id': job_metadata['user-id']
    }
    if job_metadata['is_table']:
      launched_job['task-id'] = launched_tasks
    return launched_job

  def get_jobs(self,
               status_list,
               user_list=None,
               job_list=None,
               task_list=None,
               max_jobs=0):
    """Return a list of operations based on the input criteria.

    If any of the filters are empty or "*", then no filtering is performed on
    that field.

    Args:
      status_list: a list of job status strings to return. Valid status strings
        are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_list: a list of ids for the user(s) who launched the job.
      job_list: a list of job ids to return.
      task_list: a list of specific tasks within the specified job(s) to return.
      max_jobs: the maximum number of jobs to return or 0 for no limit.

    Returns:
      A list of Genomics API Operations objects.
    """

    # Server-side, we can filter on status, job_id, user_id, task_id, but there
    # is no OR filter (only AND), and so we can't handle lists server side.
    # In practice we don't expect combinations of user lists and job lists.
    # For now, do the most brain-dead thing and if we find a common use-case
    # that performs poorly, we can re-evaluate.

    if not status_list:
      status_list = ['*']
    if not user_list:
      user_list = ['*']
    if not job_list:
      job_list = ['*']
    if not task_list:
      task_list = ['*']

    operations = []
    for status in status_list:
      for job_id in job_list:
        for user_id in user_list:
          for task_id in task_list:
            ops_filter = _Operations.get_filter(
                self._project,
                status=status,
                user_id=user_id,
                job_id=job_id,
                task_id=task_id)

            ops = _Operations.list(self._service, ops_filter, max_jobs)
            for o in ops:
              o['metadata']['job-status'] = _Operations.operation_status(o)

            if ops:
              operations.extend(ops)

            if max_jobs and len(operations) > max_jobs:
              del operations[max_jobs:]
              return operations

    return operations

  def delete_jobs(self, user_list, job_list, task_list):
    """Kills the operations associated with the specified job or job.task.

    Args:
      user_list: List of user ids who "own" the job(s) to cancel.
      job_list: List of job_ids to cancel.
      task_list: List of task-ids to cancel.

    Returns:
      A list of jobs canceled and a list of error messages.
    """
    # Look up the job(s)
    ops = self.get_jobs(
        ['RUNNING'],
        max_jobs=0,
        user_list=user_list,
        job_list=job_list,
        task_list=task_list)

    return _Operations.cancel(self._service, ops)

  def get_job_field(self, job, field):
    return _Operations.get_operation_field(job, field)

  def get_job_status_message(self, op):
    return _Operations.operation_status_message(op)

  def get_job_completion_messages(self, ops):
    error_messages = []
    for op in ops:
      _Operations.append_operation_error(error_messages, op)

    return error_messages


if __name__ == '__main__':
  pass
