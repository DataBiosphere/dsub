# Lint as: python2, python3
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
"""Submit batch jobs.

Follows the model of bsub, qsub, srun, etc.
"""

from __future__ import print_function

import argparse
import collections
import datetime
import os
import re
import sys
import time
import uuid
from dateutil.tz import tzlocal

from ..lib import dsub_errors
from ..lib import dsub_util
from ..lib import job_model
from ..lib import output_formatter
from ..lib import param_util
from ..lib import resources
from ..lib.dsub_util import print_error
from ..providers import google_base
from ..providers import provider_base

SLEEP_FUNCTION = time.sleep  # so we can replace it in tests

# When --skip is used, dsub will skip launching a new job if the outputs
# already exist. In that case, dsub returns a special job-id ("NO_JOB")
# such that callers using:
#
#   JOB_ID=$(dsub ... --skip)
#
# can safely call:
#
#   dsub ... --after $JOB_ID
#
# "NO_JOB" will be treated as having completed.
#
# The job created by dsub will automatically include a data disk,
# Each provider sets a different DATA_ROOT environment variable.
# The DATA_ROOT is the root directory for the data disk.
#
# Input arguments will, by default, be localized to ${DATA_ROOT}/input.
# Output arguments will, by default, be de-localized from ${DATA_ROOT}/output.
#
# The local paths will be automatically set to mirror the remote path.
# For example:
#    gs://bucket/path/file
#
# will become:
#    ${DATA_ROOT}/input/gs/bucket/path/file
#
# On the command-line the file arguments can be specified as either:
#
#   remote_uri
#   NAME=remote_uri
#
# If no NAME is specified, one is automatically generated, of the form
# INPUT_<n> where <n> starts at 0 for the first parsed input parameter.
#
# Example inputs:
#
# * to copy a set of BAM files into ${DATA_ROOT}/input, set:
#
#   --input gs://bucket/path/*.bam
#
# The contents will be written to (and the input parameter set to):
#
#   ${DATA_ROOT}/input/gs/bucket/path/
#
# * to copy a single BAM file to ${DATA_ROOT}/input, set:
#
#   --input gs://bucket/path/NA12878.bam
#
# The contents will be written to (and the input parameter set to):
#
#   ${DATA_ROOT}/input/gs/bucket/path/NA12878.bam
#
# Example outputs:
#
# * to copy out all BAM index files from ${DATA_ROOT}/output/gs/bucket/path/,
#   set:
#
#   --output gs://bucket/path/*.bai
#
# * to copy out a single BAM index file,
#   ${DATA_ROOT}/output/gs/bucket/path/sample.bam.bai, set:
#
#   --output gs://bucket/path/sample.bam.bai
#
# Similar functionality is available in the header row of a TSV file:
#
#   --input
#   --input VAR
#
#   --output
#   --output VAR

DEFAULT_INPUT_LOCAL_PATH = 'input'
DEFAULT_OUTPUT_LOCAL_PATH = 'output'
DEFAULT_MOUNT_LOCAL_PATH = 'mount'


class TaskParamAction(argparse.Action):
  """Parse the task flag value into a dict."""

  def __init__(self, option_strings, dest, **kwargs):
    super(TaskParamAction, self).__init__(option_strings, dest, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    # Input should contain one or two space-separated tokens:
    #
    # * a file path (required)
    # * a numeric range (optional)
    #
    # The numeric range can be any of:
    #
    #   m, m-, m-n
    #
    # There is no support for "-n" notation. If we were to add it, we'd need
    # to define its meaning ("tasks 1 through n" or "last n tasks"?) and
    # to require the entire parameter value to be quoted as the bare-word "-n"
    # would be parsed by the argument parser as a new flag.
    #
    # Inputs are turned into a dict:
    #   { 'path': path, 'task_min': m, 'task_max': n }
    #
    # We always set both min and max.
    # If one of the values is not provided, set it to None.
    # If a single value (not a range) is provided, then min == max

    if len(values) > 2:
      raise ValueError('More than 2 arguments passed to --%s' % self.dest)

    tasks = {}
    if len(values) == 2:
      path = values[0]
      task_range = values[1]
    else:
      path = values[0]
      task_range = None

    tasks['path'] = path

    if task_range:
      if '-' in task_range:
        if task_range.startswith('-'):
          raise ValueError('Task range minimum must be set')

        tasks['min'], tasks['max'] = [
            int(val) if val else None for val in task_range.split('-', 1)
        ]
      else:
        tasks['min'] = int(task_range)
        tasks['max'] = int(task_range)

    setattr(namespace, self.dest, tasks)


def _check_private_address(args):
  """If --use-private-address is enabled, ensure the Docker path is for GCR."""
  if args.use_private_address:
    split = args.image.split('/', 1)
    if len(split) == 1 or not split[0].endswith('gcr.io'):
      raise ValueError(
          '--use-private-address must specify a --image with a gcr.io host')


def _google_cls_v2_parse_arguments(args):
  """Validated google-cls-v2 arguments."""

  # For the google-cls-v2 provider, the addition of the "--location" parameter,
  # along with a default (us-central1), we can just default everything.

  # So we only need to validate that there is not both a region and zone.
  if (args.zones and args.regions):
    raise ValueError('At most one of --regions and --zones may be specified')

  if args.machine_type and (args.min_cores or args.min_ram):
    raise ValueError(
        '--machine-type not supported together with --min-cores or --min-ram.')

  _check_private_address(args)


def _google_v2_parse_arguments(args):
  """Validated google-v2 arguments."""
  if (args.zones and args.regions) or (not args.zones and not args.regions):
    raise ValueError('Exactly one of --regions and --zones must be specified')

  if args.machine_type and (args.min_cores or args.min_ram):
    raise ValueError(
        '--machine-type not supported together with --min-cores or --min-ram.')

  _check_private_address(args)


def _local_parse_arguments(args):
  """Validated local arguments."""
  if args.user and args.user != dsub_util.get_os_user():
    raise ValueError('If specified, the local provider\'s "--user" flag must '
                     'match the current logged-in user.')


def _parse_arguments(prog, argv):
  """Parses command line arguments.

  Args:
    prog: The path of the program (dsub.py) or an alternate program name to
    display in usage.
    argv: The list of program arguments to parse.

  Returns:
    A Namespace of parsed arguments.
  """
  # Handle version flag and exit if it was passed.
  param_util.handle_version_flag()

  parser = provider_base.create_parser(prog)

  # Add dsub core job submission arguments
  parser.add_argument(
      '--version', '-v', default=False, help='Print the dsub version and exit.')

  parser.add_argument(
      '--unique-job-id',
      default=False,
      action='store_true',
      help="""Experimental: create a unique 32 character UUID for the dsub
          job-id using https://docs.python.org/3/library/uuid.html.
          (default: False)""")
  parser.add_argument(
      '--name',
      help="""Name for the job. Defaults to the script name or
          first token of the --command if specified.""")
  parser.add_argument(
      '--tasks',
      nargs='*',
      action=TaskParamAction,
      help="""Path to a file of tab separated values (TSV) for task parameters.
          The file may be located in the local filesystem or in a Google Cloud
          Storage bucket.

          The first line is a list of column headers specifying an --env,
          --input, --input-recursive, --output or --output-recursive variable,
          and each subsequent line specifies the values for a task.

          Optionally specify tasks from the file to submit. Can take the form
          "m", "m-", or "m-n" where m and n are task numbers starting at 1.
          (default: None)""",
      metavar='FILE M-N')
  parser.add_argument(
      '--image',
      default='ubuntu:14.04',
      help="""Image name from Docker Hub, Google Container Repository, or other
          Docker image service. The task must have READ access to the
          image. (default: ubuntu:14.04)""")
  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Print the task(s) that would be run and then exit. (default: False)'
  )
  parser.add_argument(
      '--command',
      help="""Command to run inside the job\'s Docker container. This argument
          or the --script argument must be provided.""",
      metavar='COMMAND')
  parser.add_argument(
      '--script',
      help="""Path to a script that is located in the local file system or
          inside a Google Cloud Storage bucket. This script will be run inside
          the job\'s Docker container.  This argument or the --command
          argument must be provided.""",
      metavar='SCRIPT')
  parser.add_argument(
      '--env',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help='Environment variables for the script\'s execution environment',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--label',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help='Labels to associate to the job.',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--input',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help="""Input path arguments to localize into the script's execution
          environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--input-recursive',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help="""Input path arguments to localize recursively into the script\'s
          execution environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help="""Output path arguments to de-localize from the script\'s execution
          environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output-recursive',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help="""Output path arguments to de-localize recursively from the script's
          execution environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--user',
      '-u',
      help='User submitting the dsub job, defaults to the current OS user.')
  parser.add_argument(
      '--user-project',
      help="""Specify a user project to be billed for all requests to Google
         Cloud Storage (logging, localization, delocalization). This flag exists
         to support accessing Requester Pays buckets (default: None)""")
  parser.add_argument(
      '--mount',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help="""Mount a resource such as a bucket, disk, or directory into your
         Docker container""",
      metavar='KEY=PATH_SPEC')

  # Add dsub job management arguments
  parser.add_argument(
      '--wait',
      action='store_true',
      help='Wait for the job to finish all its tasks. (default: False)')
  parser.add_argument(
      '--retries',
      default=0,
      type=int,
      help='Number of retries to perform on failed tasks. (default: 0)')
  parser.add_argument(
      '--poll-interval',
      default=10,
      type=int,
      help='Polling interval (in seconds) for checking job status '
      'when --wait or --after are set. (default: 10)')
  parser.add_argument(
      '--after',
      nargs='+',
      default=[],
      help='Job ID(s) to wait for before starting this job.')
  parser.add_argument(
      '--skip',
      default=False,
      action='store_true',
      help="""Do not submit the job if all output specified using the --output
          and --output-recursive parameters already exist. Note that wildcard
          and recursive outputs cannot be strictly verified. See the
          documentation for details. (default: False)""")
  parser.add_argument(
      '--summary',
      default=False,
      action='store_true',
      help="""During the --wait loop, display a summary of the results,
          grouped by (job, status). (default: False)""")

  # Add dsub resource requirement arguments
  parser.add_argument(
      '--min-cores',
      type=int,
      help="""Minimum CPU cores for each job. The default is provider-specific.
           The google-v2 provider default is 1 core.
           The local provider does not allocate resources, but uses available
           resources of your machine.""")
  parser.add_argument(
      '--min-ram',
      type=float,
      help="""Minimum RAM per job in GB. The default is provider-specific.
           The google-v2 provider default is 3.75 GB.
           The local provider does not allocate resources, but uses available
           resources of your machine.""")
  parser.add_argument(
      '--disk-size',
      default=job_model.DEFAULT_DISK_SIZE,
      type=int,
      help='Size (in GB) of data disk to attach for each job (default: {})'
      .format(job_model.DEFAULT_DISK_SIZE))

  parser.add_argument(
      '--logging',
      help='Cloud Storage path to send logging output'
      ' (either a folder, or file ending in ".log")')

  # Add provider-specific arguments

  # Shared between the "google-cls-v2" and "google-v2" providers
  google_common = parser.add_argument_group(
      title='google-common',
      description="""Options common to the "google-cls-v2" and "google-v2"
        providers""")
  google_common.add_argument(
      '--project', help='Cloud project ID in which to run the job')
  google_common.add_argument(
      '--boot-disk-size',
      default=job_model.DEFAULT_BOOT_DISK_SIZE,
      type=int,
      help='Size (in GB) of the boot disk (default: {})'.format(
          job_model.DEFAULT_BOOT_DISK_SIZE))
  google_common.add_argument(
      '--preemptible',
      const=param_util.preemptile_param_type(True),
      default=param_util.preemptile_param_type(False),
      nargs='?',  # Be careful if we ever add positional arguments
      type=param_util.preemptile_param_type,
      help="""If --preemptible is given without a number, enables preemptible
          VMs for all attempts for all tasks. If a number value N is used,
          enables preemptible VMs for up to N attempts for each task.
          Defaults to not using preemptible VMs.""")
  google_common.add_argument(
      '--zones', nargs='+', help='List of Google Compute Engine zones.')
  google_common.add_argument(
      '--scopes',
      nargs='+',
      help="""Space-separated scopes for Google Compute Engine instances.
          If unspecified, provider will use '%s'""" % ','.join(
              google_base.DEFAULT_SCOPES))
  google_common.add_argument(
      '--accelerator-type',
      help="""The Compute Engine accelerator type. By specifying this parameter,
          you will download and install the following third-party software onto
          your job's Compute Engine instances: NVIDIA(R) Tesla(R) drivers and
          NVIDIA(R) CUDA toolkit. Please see
          https://cloud.google.com/compute/docs/gpus/ for supported GPU types
          and
          https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run#accelerator
          for more details. (default: None)""")
  google_common.add_argument(
      '--accelerator-count',
      type=int,
      default=0,
      help="""The number of accelerators of the specified type to attach.
          By specifying this parameter, you will download and install the
          following third-party software onto your job's Compute Engine
          instances: NVIDIA(R) Tesla(R) drivers and NVIDIA(R) CUDA toolkit.
          (default: 0)""")
  google_common.add_argument(
      '--credentials-file',
      type=str,
      help='Path to a local file with JSON credentials for a service account.')
  google_common.add_argument(
      '--regions',
      nargs='+',
      help="""List of Google Compute Engine regions.
          Only one of --zones and --regions may be specified.""")
  google_common.add_argument(
      '--machine-type', help='Provider-specific machine type (default: None)')
  google_common.add_argument(
      '--cpu-platform',
      help="""The CPU platform to request. Supported values can be found at
      https://cloud.google.com/compute/docs/instances/specify-min-cpu-platform
      (default: None)""")
  google_common.add_argument(
      '--network',
      help="""The Compute Engine VPC network name to attach the VM's network
          interface to. The value will be prefixed with global/networks/ unless
          it contains a /, in which case it is assumed to be a fully specified
          network resource URL. (default: None)""")
  google_common.add_argument(
      '--subnetwork',
      help="""The name of the Compute Engine subnetwork to attach the instance
          to. (default: None)""")
  google_common.add_argument(
      '--use-private-address',
      default=False,
      action='store_true',
      help="""If set to true, do not attach a public IP address to the VM.
      (default: False)""")
  google_common.add_argument(
      '--timeout',
      help="""The maximum amount of time to give the task to complete.
          This includes the time spent waiting for a worker to be allocated.
          Time can be listed using a number followed by a unit. Supported units
          are s (seconds), m (minutes), h (hours), d (days), w (weeks). The
          provider-specific default is 7 days. Example: '7d' (7 days).""")
  google_common.add_argument(
      '--log-interval',
      help="""The amount of time to sleep between copies of log files from
          the task to the logging path.
          Time can be listed using a number followed by a unit. Supported units
          are s (seconds), m (minutes), h (hours).
          Example: '5m' (5 minutes). Default is '1m'.""")
  google_common.add_argument(
      '--ssh',
      default=False,
      action='store_true',
      help="""If set to true, start an ssh container in the background
          to allow you to log in using SSH and debug in real time.
          (default: False)""")
  google_common.add_argument(
      '--nvidia-driver-version',
      help="""The NVIDIA driver version to use when attaching an NVIDIA GPU
          accelerator. The version specified here must be compatible with the
          GPU libraries contained in the container being executed, and must be
          one of the drivers hosted in the nvidia-drivers-us-public bucket on
          Google Cloud Storage. (default: None)""")
  google_common.add_argument(
      '--service-account',
      type=str,
      help="""Email address of the service account to be authorized on the
          Compute Engine VM for each job task. If not specified, the default
          Compute Engine service account for the project will be used.""")
  google_common.add_argument(
      '--disk-type',
      help="""
          The disk type to use for the data disk. Valid values are pd-standard
          pd-ssd and local-ssd. The default value is pd-standard.""")
  google_common.add_argument(
      '--enable-stackdriver-monitoring',
      default=False,
      action='store_true',
      help="""If set to true, enables Stackdriver monitoring on the VM.
              (default: False)""")

  google_cls_v2 = parser.add_argument_group(
      title='"google-cls-v2" provider options',
      description='See also the "google-common" options listed above')
  google_cls_v2.add_argument(
      '--location',
      default=job_model.DEFAULT_LOCATION,
      help="""Specifies the Google Cloud region to which the pipeline request
        will be sent and where operation metadata will be stored. The associated
        dsub task may be executed in another region if the --regions or --zones
        arguments are specified. (default: {})""".format(
            job_model.DEFAULT_LOCATION))

  args = provider_base.parse_args(
      parser, {
          'google-cls-v2': ['project', 'logging'],
          'google-v2': ['project', 'logging'],
          'test-fails': [],
          'local': ['logging'],
      }, argv)

  if args.provider == 'google-cls-v2':
    _google_cls_v2_parse_arguments(args)
  if args.provider == 'google-v2':
    _google_v2_parse_arguments(args)

  return args


def _get_job_resources(args):
  """Extract job-global resources requirements from input args.

  Args:
    args: parsed command-line arguments

  Returns:
    Resources object containing the requested resources for the job
  """
  logging = param_util.build_logging_param(
      args.logging) if args.logging else None
  timeout = param_util.timeout_in_seconds(args.timeout)
  log_interval = param_util.log_interval_in_seconds(args.log_interval)

  return job_model.Resources(
      min_cores=args.min_cores,
      min_ram=args.min_ram,
      machine_type=args.machine_type,
      disk_size=args.disk_size,
      disk_type=args.disk_type,
      boot_disk_size=args.boot_disk_size,
      image=args.image,
      regions=args.regions,
      zones=args.zones,
      logging=logging,
      logging_path=None,
      service_account=args.service_account,
      scopes=args.scopes,
      cpu_platform=args.cpu_platform,
      network=args.network,
      subnetwork=args.subnetwork,
      use_private_address=args.use_private_address,
      accelerator_type=args.accelerator_type,
      accelerator_count=args.accelerator_count,
      nvidia_driver_version=args.nvidia_driver_version,
      timeout=timeout,
      log_interval=log_interval,
      ssh=args.ssh,
      enable_stackdriver_monitoring=args.enable_stackdriver_monitoring,
      max_retries=args.retries,
      max_preemptible_attempts=args.preemptible)


def _get_job_metadata(provider, user_id, job_name, script, task_ids,
                      user_project, unique_job_id):
  """Allow provider to extract job-specific metadata from command-line args.

  Args:
    provider: job service provider
    user_id: user submitting the job
    job_name: name for the job
    script: the script to run
    task_ids: a set of the task-ids for all tasks in the job
    user_project: name of the project to be billed for the request
    unique_job_id: generate a unique job id

  Returns:
    A dictionary of job-specific metadata (such as job id, name, etc.)
  """
  create_time = dsub_util.replace_timezone(datetime.datetime.now(), tzlocal())
  user_id = user_id or dsub_util.get_os_user()
  job_metadata = provider.prepare_job_metadata(script.name, job_name, user_id)
  if unique_job_id:
    job_metadata['job-id'] = uuid.uuid4().hex
  else:
    # Build the job-id. We want the job-id to be expressive while also
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
    job_metadata['job-id'] = '%s--%s--%s' % (
        job_metadata['job-name'][:10], job_metadata['user-id'],
        create_time.strftime('%y%m%d-%H%M%S-%f')[:16])

  job_metadata['create-time'] = create_time
  job_metadata['script'] = script
  job_metadata['user-project'] = user_project
  if task_ids:
    job_metadata['task-ids'] = dsub_util.compact_interval_string(list(task_ids))

  return job_metadata


def _resolve_task_logging(job_metadata, job_resources, task_descriptors):
  """Resolve the logging path from job and task properties.

  Args:
    job_metadata: Job metadata, such as job-id, job-name, and user-id.
    job_resources: Resources specified such as ram, cpu, and logging path.
    task_descriptors: Task metadata, parameters, and resources.

  Resolve the logging path, which may have substitution parameters such as
  job-id, task-id, user-id, and job-name.
  """
  if not job_resources.logging:
    return

  for task_descriptor in task_descriptors:
    logging_uri = provider_base.format_logging_uri(
        job_resources.logging.uri, job_metadata, task_descriptor.task_metadata)
    logging_path = job_model.LoggingParam(logging_uri,
                                          job_resources.logging.file_provider)

    if task_descriptor.task_resources:
      task_descriptor.task_resources = task_descriptor.task_resources._replace(
          logging_path=logging_path)
    else:
      task_descriptor.task_resources = job_model.Resources(
          logging_path=logging_path)


def _resolve_preemptible(job_resources, task_descriptors):
  """Resolve whether or not to use a preemptible machine.

  Args:
    job_resources: Resources specified such as max_preemptible_attempts.
    task_descriptors: Task metadata, parameters, and resources.
  """
  # Determine if the next attempt should be preemptible
  for task_descriptor in task_descriptors:
    # The original attempt is attempt number 1.
    # The first retry is attempt number 2.
    attempt_number = task_descriptor.task_metadata.get('task-attempt', 1)
    max_preemptible_attempts = job_resources.max_preemptible_attempts
    if max_preemptible_attempts:
      use_preemptible = max_preemptible_attempts.should_use_preemptible(
          attempt_number)
    else:
      use_preemptible = job_model.DEFAULT_PREEMPTIBLE
    task_descriptor.task_resources = task_descriptor.task_resources._replace(
        preemptible=use_preemptible)


def _resolve_task_resources(job_metadata, job_resources, task_descriptors):
  """Resolve task properties (such as the logging path) from job properties.

  Args:
    job_metadata: Job metadata, such as job-id, job-name, and user-id.
    job_resources: Resources specified such as ram, cpu, and logging path.
    task_descriptors: Task metadata, parameters, and resources.  This function
      exists to be called at the point that all job properties have been
      validated and resolved. It is also called prior to re-trying a task.
  Right now we resolve two properties: 1) the logging path, which may have
    substitution parameters such as job-id, task-id, task-attempt, user-id, and
    job-name. and 2) preemptible, which depends on how many preemptible attempts
    we have done.
  """
  _resolve_task_logging(job_metadata, job_resources, task_descriptors)
  _resolve_preemptible(job_resources, task_descriptors)


def _wait_after(provider, job_ids, poll_interval, stop_on_failure, summary):
  """Print status info as we wait for those jobs.

  Blocks until either all of the listed jobs succeed,
  or one of them fails.

  Args:
    provider: job service provider
    job_ids: a set of job IDs (string) to wait for
    poll_interval: integer seconds to wait between iterations
    stop_on_failure: whether to stop waiting if one of the tasks fails.
    summary: whether to output summary messages

  Returns:
    Empty list if there was no error,
    a list of error messages from the failed tasks otherwise.
  """

  # Each time through the loop, the job_set is re-set to the jobs remaining to
  # check. Jobs are removed from the list when they complete.
  #
  # We exit the loop when:
  # * No jobs remain are running, OR
  # * stop_on_failure is TRUE AND at least one job returned an error

  # remove NO_JOB
  job_ids_to_check = {j for j in job_ids if j != dsub_util.NO_JOB}
  error_messages = []
  while job_ids_to_check and (not error_messages or not stop_on_failure):
    print('Waiting for: %s.' % (', '.join(job_ids_to_check)))

    # Poll until any remaining jobs have completed
    jobs_left = _wait_for_any_job(provider, job_ids_to_check, poll_interval,
                                  summary)

    # Calculate which jobs just completed
    jobs_completed = job_ids_to_check.difference(jobs_left)

    # Get all tasks for the newly completed jobs
    tasks_completed = provider.lookup_job_tasks({'*'},
                                                job_ids=jobs_completed,
                                                verbose=False)

    # We don't want to overwhelm the user with output when there are many
    # tasks per job. So we get a single "dominant" task for each of the
    # completed jobs (one that is representative of the job's fate).
    dominant_job_tasks = _dominant_task_for_jobs(tasks_completed)
    if len(dominant_job_tasks) != len(jobs_completed):
      # print info about the jobs we couldn't find
      # (should only occur for "--after" where the job ID is a typo).
      jobs_found = dsub_util.tasks_to_job_ids(dominant_job_tasks)
      jobs_not_found = jobs_completed.difference(jobs_found)
      for j in jobs_not_found:
        error = '%s: not found' % j
        print_error('  %s' % error)
        error_messages += [error]

    # Print the dominant task for the completed jobs
    for t in dominant_job_tasks:
      job_id = t.get_field('job-id')
      status = t.get_field('task-status')
      print('  %s: %s' % (str(job_id), str(status)))
      if status in ['FAILURE', 'CANCELED']:
        error_messages += [provider.get_tasks_completion_messages([t])]

    job_ids_to_check = jobs_left

  return error_messages


def _wait_and_retry(provider, job_id, poll_interval, retries, job_descriptor,
                    summary):
  """Wait for job and retry any tasks that fail.

  Stops retrying an individual task when: it succeeds, is canceled, or has been
  retried "retries" times.

  This function exits when there are no tasks running and there are no tasks
  eligible to be retried.

  Args:
    provider: job service provider
    job_id: a single job ID (string) to wait for
    poll_interval: integer seconds to wait between iterations
    retries: number of retries
    job_descriptor: job descriptor used to originally submit job
    summary: whether to output summary messages

  Returns:
    Empty list if there was no error,
    a list containing an error message from a failed task otherwise.
  """

  while True:
    formatted_tasks = []
    tasks = provider.lookup_job_tasks({'*'}, job_ids=[job_id], verbose=False)

    running_tasks = set()
    completed_tasks = set()
    canceled_tasks = set()
    fully_failed_tasks = set()
    task_fail_count = dict()

    # This is an arbitrary task that is either fully failed or canceled (with
    # preference for the former).
    message_task = None

    task_dict = dict()
    for t in tasks:
      task_id = t.get_field('task-id')
      if task_id is not None:
        task_id = int(task_id)

      task_dict[task_id] = t

      status = t.get_field('task-status')
      if status == 'FAILURE':
        # Could compute this from task-attempt as well.
        task_fail_count[task_id] = task_fail_count.get(task_id, 0) + 1
        if task_fail_count[task_id] > retries:
          fully_failed_tasks.add(task_id)
          message_task = t
      elif status == 'CANCELED':
        canceled_tasks.add(task_id)
        if not message_task:
          message_task = t
      elif status == 'SUCCESS':
        completed_tasks.add(task_id)
      elif status == 'RUNNING':
        running_tasks.add(task_id)

      if summary:
        formatted_tasks.append(
            output_formatter.prepare_row(t, full=False, summary=True))

    if summary:
      formatter = output_formatter.TextOutput(full=False)
      formatter.prepare_and_print_table(formatted_tasks, summary)

    retry_tasks = (
        set(task_fail_count).difference(fully_failed_tasks)
        .difference(running_tasks).difference(completed_tasks)
        .difference(canceled_tasks))

    # job completed.
    if not retry_tasks and not running_tasks:
      # If there are any fully failed tasks, return the completion message of an
      # arbitrary one.
      # If not, but there are canceled tasks, return the completion message of
      # an arbitrary one.
      if message_task:
        return [provider.get_tasks_completion_messages([message_task])]

      # Otherwise successful completion.
      return []

    for task_id in retry_tasks:
      identifier = '{}.{}'.format(job_id, task_id) if task_id else job_id
      print('  {} (attempt {}) failed. Retrying.'.format(
          identifier, task_fail_count[task_id]))
      msg = task_dict[task_id].get_field('status-message')
      print('  Failure message: ' + msg)

      _retry_task(provider, job_descriptor, task_id,
                  task_fail_count[task_id] + 1)

    SLEEP_FUNCTION(poll_interval)


def _retry_task(provider, job_descriptor, task_id, task_attempt):
  """Retry task_id (numeric id) assigning it task_attempt."""
  td_orig = job_descriptor.find_task_descriptor(task_id)

  new_task_descriptors = [
      job_model.TaskDescriptor({
          'task-id': task_id,
          'task-attempt': task_attempt
      }, td_orig.task_params, td_orig.task_resources)
  ]

  # Update the logging path and preemptible field.
  _resolve_task_resources(job_descriptor.job_metadata,
                          job_descriptor.job_resources, new_task_descriptors)

  provider.submit_job(
      job_model.JobDescriptor(
          job_descriptor.job_metadata, job_descriptor.job_params,
          job_descriptor.job_resources, new_task_descriptors), False)


def _dominant_task_for_jobs(tasks):
  """A list with, for each job, its dominant task.

  The dominant task is the one that exemplifies its job's
  status. It is either:
  - the first (FAILURE or CANCELED) task, or if none
  - the first RUNNING task, or if none
  - the first SUCCESS task.

  Args:
    tasks: a list of tasks to consider

  Returns:
    A list with, for each job, its dominant task.
  """

  per_job = _group_tasks_by_jobid(tasks)

  ret = []
  for job_id in per_job.keys():
    tasks_in_salience_order = sorted(per_job[job_id], key=_importance_of_task)
    ret.append(tasks_in_salience_order[0])
  return ret


def _group_tasks_by_jobid(tasks):
  """A defaultdict with, for each job, a list of its tasks."""
  ret = collections.defaultdict(list)
  for t in tasks:
    ret[t.get_field('job-id')].append(t)
  return ret


def _importance_of_task(task):
  """Tuple (importance, end-time). Smaller values are more important."""
  # The status of a job is going to be determined by the roll-up of its tasks.
  # A FAILURE or CANCELED task means the job has FAILED.
  # If none, then any RUNNING task, the job is still RUNNING.
  # If none, then the job status is SUCCESS.
  #
  # Thus the dominant task for each job is one that exemplifies its
  # status:
  #
  # 1- The first (FAILURE or CANCELED) task, or if none
  # 2- The first RUNNING task, or if none
  # 3- The first SUCCESS task.
  importance = {'FAILURE': 0, 'CANCELED': 0, 'RUNNING': 1, 'SUCCESS': 2}
  return (importance[task.get_field('task-status')],
          task.get_field(
              'end-time',
              dsub_util.replace_timezone(datetime.datetime.max, tzlocal())))


def _wait_for_any_job(provider, job_ids, poll_interval, summary):
  """Waits until any of the listed jobs is not running.

  In particular, if any of the jobs sees one of its tasks fail,
  we count the whole job as failing (but do not terminate the remaining
  tasks ourselves).

  Args:
    provider: job service provider
    job_ids: a list of job IDs (string) to wait for
    poll_interval: integer seconds to wait between iterations
    summary: whether to output summary messages

  Returns:
    A set of the jobIDs with still at least one running task.
  """
  if not job_ids:
    return
  while True:
    formatted_tasks = []
    tasks = provider.lookup_job_tasks({'*'}, job_ids=job_ids, verbose=False)
    running_jobs = set()
    failed_jobs = set()
    for t in tasks:
      status = t.get_field('task-status')
      job_id = t.get_field('job-id')
      if status in ['FAILURE', 'CANCELED']:
        failed_jobs.add(job_id)
      if status == 'RUNNING':
        running_jobs.add(job_id)

      if summary:
        formatted_tasks.append(
            output_formatter.prepare_row(t, full=False, summary=True))

    if summary:
      formatter = output_formatter.TextOutput(full=False)
      formatter.prepare_and_print_table(formatted_tasks, summary)

    remaining_jobs = running_jobs.difference(failed_jobs)
    if failed_jobs or len(remaining_jobs) != len(job_ids):
      return remaining_jobs
    SLEEP_FUNCTION(poll_interval)


def _validate_job_and_task_arguments(job_params, task_descriptors):
  """Validates that job and task argument names do not overlap."""

  if not task_descriptors:
    return

  task_params = task_descriptors[0].task_params

  # The use case for specifying a label or env/input/output parameter on
  # the command-line and also including it in the --tasks file is not obvious.
  # Should the command-line override the --tasks file? Why?
  # Until this use is articulated, generate an error on overlapping names.

  # Check labels
  from_jobs = {label.name for label in job_params['labels']}
  from_tasks = {label.name for label in task_params['labels']}

  intersect = from_jobs & from_tasks
  if intersect:
    raise ValueError(
        'Names for labels on the command-line and in the --tasks file must not '
        'be repeated: {}'.format(','.join(intersect)))

  # Check envs, inputs, and outputs, all of which must not overlap each other
  from_jobs = {
      item.name
      for item in job_params['envs'] | job_params['inputs']
      | job_params['outputs']
  }
  from_tasks = {
      item.name
      for item in task_params['envs'] | task_params['inputs']
      | task_params['outputs']
  }

  intersect = from_jobs & from_tasks
  if intersect:
    raise ValueError(
        'Names for envs, inputs, and outputs on the command-line and in the '
        '--tasks file must not be repeated: {}'.format(','.join(intersect)))


def dsub_main(prog, argv):
  # Parse args and validate
  args = _parse_arguments(prog, argv)
  # intent:
  # * dsub tightly controls the output to stdout.
  # * wrap the main body such that output goes to stderr.
  # * only emit the job-id to stdout (which can then be used programmatically).
  with dsub_util.replace_print():
    launched_job = run_main(args)
  print(launched_job.get('job-id', ''))
  return launched_job


def call(argv):
  return dsub_main('%s.call' % __name__, argv)


def main(prog=None, argv=None):
  if prog is None and argv is None:
    prog = sys.argv[0]
    argv = sys.argv[1:]

  try:
    dsub_main(prog, argv)
  except dsub_errors.PredecessorJobFailureError as e:
    # Never tried to launch. Failure occurred in the --after wait.
    print(dsub_util.NO_JOB)
    sys.exit(1)
  except dsub_errors.JobError as e:
    # Job was launched, but there was failure in --wait
    print('%s: %s' % (type(e).__name__, str(e)), file=sys.stderr)
    print(e.launched_job['job-id'])
    sys.exit(1)
  return 0


def run_main(args):
  """Execute job/task submission from command-line arguments."""

  if args.command and args.script:
    raise ValueError('Cannot supply both a --command and --script flag')

  provider_base.check_for_unsupported_flag(args)

  # Set up job parameters and job data from a tasks file or flags.
  input_file_param_util = param_util.InputFileParamUtil(
      DEFAULT_INPUT_LOCAL_PATH)
  output_file_param_util = param_util.OutputFileParamUtil(
      DEFAULT_OUTPUT_LOCAL_PATH)
  mount_param_util = param_util.MountParamUtil(DEFAULT_MOUNT_LOCAL_PATH)

  # Get job arguments from the command line
  job_params = param_util.args_to_job_params(
      args.env, args.label, args.input, args.input_recursive, args.output,
      args.output_recursive, args.mount, input_file_param_util,
      output_file_param_util, mount_param_util)
  # If --tasks is on the command-line, then get task-specific data
  if args.tasks:
    task_descriptors = param_util.tasks_file_to_task_descriptors(
        args.tasks, args.retries, input_file_param_util, output_file_param_util)

    # Validate job data + task data
    _validate_job_and_task_arguments(job_params, task_descriptors)
  else:
    # Create the implicit task
    task_metadata = {'task-id': None}
    if args.retries:
      task_metadata['task-attempt'] = 1
    task_descriptors = [
        job_model.TaskDescriptor(task_metadata, {
            'labels': set(),
            'envs': set(),
            'inputs': set(),
            'outputs': set()
        }, job_model.Resources())
    ]

  return run(
      provider_base.get_provider(args, resources),
      _get_job_resources(args),
      job_params,
      task_descriptors,
      name=args.name,
      dry_run=args.dry_run,
      command=args.command,
      script=args.script,
      user=args.user,
      user_project=args.user_project,
      wait=args.wait,
      retries=args.retries,
      max_preemptible_attempts=args.preemptible,
      poll_interval=args.poll_interval,
      after=args.after,
      skip=args.skip,
      project=args.project,
      location=args.location,
      disable_warning=True,
      unique_job_id=args.unique_job_id,
      summary=args.summary)


def run(provider,
        job_resources,
        job_params,
        task_descriptors,
        name=None,
        dry_run=False,
        command=None,
        script=None,
        user=None,
        user_project=None,
        wait=False,
        retries=0,
        max_preemptible_attempts=None,
        poll_interval=10,
        after=None,
        skip=False,
        project=None,
        location=None,
        disable_warning=False,
        unique_job_id=False,
        summary=False):
  """Actual dsub body, post-stdout-redirection."""
  if not dry_run:
    provider_base.emit_provider_message(provider)

  if not disable_warning:
    raise ValueError('Do not use this unstable API component!')

  if command and script:
    raise ValueError('Cannot supply both a command and script value.')

  if command:
    if name:
      command_name = name
    else:
      command_name = _name_for_command(command)

    # Add the shebang line to ensure the command is treated as Bash
    script = job_model.Script(command_name, '#!/usr/bin/env bash\n' + command)
  elif script:
    # Read the script file
    script_file_contents = dsub_util.load_file(script)
    script = job_model.Script(os.path.basename(script), script_file_contents)
  else:
    raise ValueError('One of --command or a script name must be supplied')

  if retries and not wait:
    raise ValueError('Requesting retries requires requesting wait')

  if summary and not wait:
    raise ValueError('Requesting summary requires requesting wait')

  if max_preemptible_attempts:
    max_preemptible_attempts.validate(retries)

  # The contract with providers and downstream code is that the job_params
  # and task_params contain 'labels', 'envs', 'inputs', and 'outputs'.
  job_model.ensure_job_params_are_complete(job_params)
  job_model.ensure_task_params_are_complete(task_descriptors)

  task_ids = {
      task_descriptor.task_metadata.get('task-id')
      for task_descriptor in task_descriptors
      if task_descriptor.task_metadata.get('task-id') is not None
  }

  # Job and task parameters from the user have been validated.
  # We can now compute some job and task properties, including:
  #  job_metadata such as the job-id, create-time, user-id, etc.
  #  task_resources such as the logging_path (which may include job-id, task-id)
  job_metadata = _get_job_metadata(provider, user, name, script, task_ids,
                                   user_project, unique_job_id)
  _resolve_task_resources(job_metadata, job_resources, task_descriptors)

  # Job and task properties are now all resolved. Begin execution!
  if not dry_run:
    print('Job properties:')
    print('  job-id: %s' % job_metadata['job-id'])
    print('  job-name: %s' % job_metadata['job-name'])
    print('  user-id: %s' % job_metadata['user-id'])

  # Wait for predecessor jobs (if any)
  if after:
    if dry_run:
      print('(Pretend) waiting for: %s.' % after)
    else:
      print('Waiting for predecessor jobs to complete...')
      error_messages = _wait_after(provider, after, poll_interval, True,
                                   summary)
      if error_messages:
        for msg in error_messages:
          print_error(msg)
        raise dsub_errors.PredecessorJobFailureError(
            'One or more predecessor jobs completed but did not succeed.',
            error_messages, None)

  # Launch all the job tasks!
  job_descriptor = job_model.JobDescriptor(job_metadata, job_params,
                                           job_resources, task_descriptors)
  launched_job = provider.submit_job(job_descriptor, skip)

  if not dry_run:
    if launched_job['job-id'] == dsub_util.NO_JOB:
      print('Job output already present, skipping new job submission.')
      return {'job-id': dsub_util.NO_JOB}
    print('Launched job-id: %s' % launched_job['job-id'])
    if launched_job.get('task-id'):
      print('%s task(s)' % len(launched_job['task-id']))
    print('To check the status, run:')
    print("  dstat %s --jobs '%s' --users '%s' --status '*'" %
          (provider_base.get_dstat_provider_args(provider, project, location),
           launched_job['job-id'], launched_job['user-id']))
    print('To cancel the job, run:')
    print("  ddel %s --jobs '%s' --users '%s'" %
          (provider_base.get_ddel_provider_args(provider, project, location),
           launched_job['job-id'], launched_job['user-id']))

  # Poll for job completion
  if wait:
    print('Waiting for job to complete...')

    if retries:
      print('Monitoring for failed tasks to retry...')
      print(
          '*** This dsub process must continue running to retry failed tasks.')
      error_messages = _wait_and_retry(provider, job_metadata['job-id'],
                                       poll_interval, retries, job_descriptor,
                                       summary)
    else:
      error_messages = _wait_after(provider, [job_metadata['job-id']],
                                   poll_interval, False, summary)
    if error_messages:
      for msg in error_messages:
        print_error(msg)
      raise dsub_errors.JobExecutionError(
          'One or more jobs finished with status FAILURE or CANCELED'
          ' during wait.', error_messages, launched_job)

  return launched_job


def _name_for_command(command):
  r"""Craft a simple command name from the command.

  The best command strings for this are going to be those where a simple
  command was given; we will use the command to derive the name.

  We won't always be able to figure something out and the caller should just
  specify a "--name" on the command-line.

  For example, commands like "export VAR=val\necho ${VAR}", this function would
  return "export".

  If the command starts space or a comment, then we'll skip to the first code
  we can find.

  If we find nothing, just return "command".

  >>> _name_for_command('samtools index "${BAM}"')
  'samtools'
  >>> _name_for_command('/usr/bin/sort "${INFILE}" > "${OUTFILE}"')
  'sort'
  >>> _name_for_command('# This should be ignored')
  'command'
  >>> _name_for_command('\\\n\\\n# Bad continuations, but ignore.\necho hello.')
  'echo'
  >>> _name_for_command('(uname -a && pwd) # Command begins with non-letter.')
  'uname'
  >>> _name_for_command('my-program.sh # Command with hyphens.')
  'my-program.sh'
  >>> _name_for_command('/home/user/bin/-my-sort # Path with hyphen.')
  'my-sort'

  Arguments:
    command: the user-provided command
  Returns:
    a proposed name for the task.
  """

  lines = command.splitlines()
  for line in lines:
    line = line.strip()
    if line and not line.startswith('#') and line != '\\':
      # Tokenize on whitespace [ \t\n\r\f\v]
      names = re.split(r'\s', line)
      for name in names:
        # Make sure the first character is a letter, number, or underscore
        # Get basename so something like "/usr/bin/sort" becomes just "sort"
        name = re.sub(r'^[^a-zA-Z0-9_]*', '', os.path.basename(name))
        if name:
          return name

  return 'command'


if __name__ == '__main__':
  main(sys.argv[0], sys.argv[1:])
