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

import argparse
import collections
import os
import re
import sys
import time

from ..lib import dsub_util
from ..lib import job_util
from ..lib import param_util
from ..lib.dsub_util import print_error
from ..providers import provider_base

SLEEP_FUNCTION = time.sleep  # so we can replace it in tests

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/bigquery',]

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
NO_JOB = 'NO_JOB'

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


class ListParamAction(argparse.Action):
  """Append each value as a separate element to the parser destination.

  This class refines the 'append' action argument.
  For the parameters:

    --myarg val1 val2 --myarg val3

  'append' action yields:

    args.myval = ['val1 val2', 'val3']

  ListParamAction yields:

    args.myval = ['val1', 'val2', 'val3']
  """

  def __init__(self, option_strings, dest, **kwargs):
    super(ListParamAction, self).__init__(option_strings, dest, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    params = getattr(namespace, self.dest, [])

    # Input comes in as a list (possibly len=1) of NAME=VALUE pairs
    for arg in values:
      params.append(arg)

    setattr(namespace, self.dest, params)


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


def parse_arguments(prog, argv):
  """Parses command line arguments.

  Args:
    prog: The path of the program (dsub.py) or an alternate program name to
    display in usage.
    argv: The list of program arguments to parse.

  Returns:
    A Namespace of parsed arguments.
  """

  provider_required_args = {
      'google': ['project', 'zones', 'logging'],
      'test-fails': [],
      'local': ['logging'],
  }
  epilog = 'Provider-required arguments:\n'
  for provider in provider_required_args:
    epilog += '  %s: %s\n' % (provider, provider_required_args[provider])
  parser = argparse.ArgumentParser(
      prog=prog,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      epilog=epilog)

  # Add dsub core job submission arguments
  parser.add_argument(
      '--name',
      help="""Name for pipeline. Defaults to the script name or
          first token of the --command if specified.""")
  parser.add_argument(
      '--tasks',
      nargs='*',
      action=TaskParamAction,
      help="""Path to TSV of task parameters. Each column can specify an
          --env, --input, or --output variable, and each line specifies the
          values of those variables for a separate task.

          Optionally specify tasks in the file to submit. Can take the form
          "m", "m-", or "m-n" where m and n are task numbers.""",
      metavar='FILE M-N')
  parser.add_argument(
      '--image',
      default='ubuntu:14.04',
      help="""Image name from Docker Hub, Google Container Repository, or other
          Docker image service. The pipeline must have READ access to the
          image.""")
  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Print the pipeline(s) that would be run and then exit.')
  parser.add_argument(
      '--command',
      help='Command to run inside the job\'s Docker container',
      metavar='COMMAND')
  parser.add_argument(
      '--script',
      help='Local path to a script to run inside the job\'s Docker container.',
      metavar='SCRIPT')
  parser.add_argument(
      '--env',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Environment variables for the script\'s execution environment',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--label',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Labels to associate to the job.',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--input',
      nargs='*',
      action=ListParamAction,
      default=[],
      help="""Input path arguments to localize into the script's execution
          environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--input-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help="""Input path arguments to localize recursively into the script\'s
          execution environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output',
      nargs='*',
      action=ListParamAction,
      default=[],
      help="""Output path arguments to de-localize from the script\'s execution
          environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help="""Output path arguments to de-localize recursively from the script's
          execution environment""",
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--user',
      '-u',
      default=None,
      help='User submitting the dsub job, defaults to the current OS user.')

  # Add dsub job management arguments
  parser.add_argument(
      '--wait',
      action='store_true',
      help='Wait for the job to finish all its tasks.')
  parser.add_argument(
      '--poll-interval',
      default=10,
      type=int,
      help='Polling interval (in seconds) for checking job status '
      'when --wait or --after are set.')
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
          documentation for details.""")

  # Add dsub resource requirement arguments
  parser.add_argument(
      '--min-cores', default=1, type=int, help='Minimum CPU cores for each job')
  parser.add_argument(
      '--min-ram', default=3.75, type=float, help='Minimum RAM per job in GB')
  parser.add_argument(
      '--disk-size',
      default=200,
      type=int,
      help='Size (in GB) of data disk to attach for each job')

  parser.add_argument(
      '--logging',
      help='Cloud Storage path to send logging output'
      ' (either a folder, or file ending in ".log")')

  # Add provider-specific arguments
  provider_base.add_provider_argument(parser)
  google = parser.add_argument_group(
      title='google',
      description='Options for the Google provider (Pipelines API)')
  google.add_argument(
      '--project',
      default=None,
      help='Cloud project ID in which to run the pipeline')
  google.add_argument(
      '--boot-disk-size',
      default=10,
      type=int,
      help='Size (in GB) of the boot disk')
  google.add_argument(
      '--preemptible',
      default=False,
      action='store_true',
      help='Use a preemptible VM for the job')
  google.add_argument(
      '--zones',
      default=None,
      nargs='+',
      help='List of Google Compute Engine zones.')
  google.add_argument(
      '--scopes',
      default=DEFAULT_SCOPES,
      nargs='+',
      help='Space-separated scopes for GCE instances.')

  args = parser.parse_args(argv)

  # check special flag rules
  for arg in provider_required_args[args.provider]:
    if not args.__getattribute__(arg):
      parser.error('argument --%s is required' % arg)
  return args


def get_job_resources(args):
  """Extract job-global resources requirements from input args.

  Args:
    args: parsed command-line arguments

  Returns:
    JobResources object containing the requested resources for the job
  """
  logging = param_util.build_logging_param(args.logging)
  return job_util.JobResources(
      min_cores=args.min_cores,
      min_ram=args.min_ram,
      disk_size=args.disk_size,
      boot_disk_size=args.boot_disk_size,
      preemptible=args.preemptible,
      image=args.image,
      zones=args.zones,
      logging=logging,
      scopes=args.scopes)


def get_job_metadata(args, script, provider):
  """Allow provider to extract job-specific metadata from command-line args.

  Args:
    args: parsed command-line arguments
    script: the script to run
    provider: job service provider

  Returns:
    A dictionary of job-specific metadata (such as job id, name, etc.)
  """
  user_name = args.user or dsub_util.get_os_user()
  job_metadata = provider.prepare_job_metadata(script.name, args.name,
                                               user_name)

  job_metadata['script'] = script

  return job_metadata


def wait_after(provider, jobid_list, poll_interval, stop_on_failure):
  """Print status info as we wait for those jobs.

  Blocks until either all of the listed jobs succeed,
  or one of them fails.

  Args:
    provider: job service provider
    jobid_list: a list of job IDs (string) to wait for
    poll_interval: integer seconds to wait between iterations
    stop_on_failure: whether to stop waiting if one of the tasks fails.

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
  job_set = set([j for j in jobid_list if j != NO_JOB])
  error_messages = []
  while job_set and (not error_messages or not stop_on_failure):
    print 'Waiting for: %s.' % (', '.join(job_set))

    # Poll until any remaining jobs have completed
    jobs_left = wait_for_any_job(provider, job_set, poll_interval)

    # Calculate which jobs just completed
    jobs_completed = job_set.difference(jobs_left)

    # Get all tasks for the newly completed jobs
    tasks_completed = provider.lookup_job_tasks(['*'], job_list=jobs_completed)

    # We don't want to overwhelm the user with output when there are many
    # tasks per job. So we get a single "dominant" task for each of the
    # completed jobs (one that is representative of the job's fate).
    dominant_job_tasks = dominant_task_for_jobs(provider, tasks_completed)
    if len(dominant_job_tasks) != len(jobs_completed):
      # print info about the jobs we couldn't find
      # (should only occur for "--after" where the job ID is a typo).
      jobs_found = dsub_util.tasks_to_job_ids(provider, dominant_job_tasks)
      jobs_not_found = jobs_completed.difference(jobs_found)
      for j in jobs_not_found:
        error = '%s: not found' % (j)
        print_error('  %s' % (error))
        error_messages += [error]

    # Print the dominant task for the completed jobs
    for t in dominant_job_tasks:
      job_id = provider.get_task_field(t, 'job-id')
      status = provider.get_task_field(t, 'job-status')
      print '  %s: %s' % (str(job_id), str(status))
      if status in ['FAILURE', 'CANCELED']:
        error_messages += [provider.get_tasks_completion_messages([t])]

    job_set = jobs_left

  return error_messages


def dominant_task_for_jobs(provider, tasks):
  """A list with, for each job, its dominant task.

  The dominant task is the one that exemplifies its job's
  status. It is either:
  - the first (FAILURE or CANCELED) task, or if none
  - the first RUNNING task, or if none
  - the first SUCCESS task.

  Args:
    provider: job service provider
    tasks: a list of tasks to consider

  Returns:
    A list with, for each job, its dominant task.
  """

  per_job = group_tasks_by_jobid(provider, tasks)

  ret = []
  for job_id in per_job.keys():
    tasks_in_salience_order = sorted(
        per_job[job_id], key=lambda t: importance_of_task(provider, t))
    ret.append(tasks_in_salience_order[0])
  return ret


def group_tasks_by_jobid(provider, tasks):
  """A defaultdict with, for each job, a list of its tasks."""
  ret = collections.defaultdict(list)
  for t in tasks:
    ret[provider.get_task_field(t, 'job-id')].append(t)
  return ret


def importance_of_task(provider, task):
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
  return (importance[provider.get_task_field(task, 'job-status')],
          provider.get_task_field(task, 'end-time'))


def wait_for_any_job(provider, jobid_list, poll_interval):
  """Waits until any of the listed jobs is not running.

  In particular, if any of the jobs sees one of its tasks fail,
  we count the whole job as failing (but do not terminate the remaining
  tasks ourselves).

  Args:
    provider: job service provider
    jobid_list: a list of job IDs (string) to wait for
    poll_interval: integer seconds to wait between iterations

  Returns:
    A set of the jobIDs with still at least one running task.
  """
  if not jobid_list:
    return
  while True:
    tasks = provider.lookup_job_tasks('*', job_list=jobid_list)
    running_jobs = set([])
    failed_jobs = set([])
    for t in tasks:
      status = provider.get_task_field(t, 'job-status')
      job_id = provider.get_task_field(t, 'job-id')
      if status in ['FAILURE', 'CANCELED']:
        failed_jobs.add(job_id)
      if status == 'RUNNING':
        running_jobs.add(job_id)
    remaining_jobs = running_jobs.difference(failed_jobs)
    if failed_jobs or len(remaining_jobs) != len(jobid_list):
      return remaining_jobs
    SLEEP_FUNCTION(poll_interval)


def _job_outputs_are_present(job_data):
  """True if each output contains at least one file."""
  # See reference args_to_job_data in param_util.py for a description
  # of what's in job_data.
  outputs = job_data['outputs']
  for o in outputs:
    if o.recursive:
      if not dsub_util.folder_exists(o.value):
        return False
    else:
      if not dsub_util.simple_pattern_exists_in_gcs(o.value):
        return False
  return True


def dsub_main(prog, argv):
  # Parse args and validate
  args = parse_arguments(prog, argv)
  # intent:
  # * dsub tightly controls the output to stdout.
  # * wrap the main body such that output goes to stderr.
  # * only emit the job-id to stdout (which can then be used programmatically).
  with dsub_util.replace_print():
    launched_job = run_main(args)
  print launched_job.get('job-id', '')
  return launched_job


def call(argv):
  return dsub_main('%s.call' % __name__, argv)


def main(prog=sys.argv[0], argv=sys.argv[1:]):
  dsub_main(prog, argv)
  return 0


def run_main(args):
  """Actual dsub body, post-stdout-redirection."""
  if args.command and args.script:
    raise ValueError('Cannot supply both --command and a script name.')

  if (args.env or args.input or args.input_recursive or args.output or
      args.output_recursive) and args.tasks:
    raise ValueError('Cannot supply both command-line parameters '
                     '(--env/--input/--input-recursive/--output/'
                     '--output-recursive) and --tasks')

  if args.tasks and args.skip:
    raise ValueError('Output skipping (--skip) not supported for --task '
                     'commands.')

  provider_base.check_for_unsupported_flag(args)

  if args.command:
    if args.name:
      command_name = args.name
    else:
      command_name = _name_for_command(args.command)

    # add the shebang line to ensure the script's run.
    script = job_util.Script(command_name, '#!/bin/bash\n' + args.command)
  elif args.script:
    # Read the script file
    script_file = dsub_util.load_file(args.script)
    script = job_util.Script(os.path.basename(args.script), script_file.read())
  else:
    raise ValueError('One of --command or a script name must be supplied')

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Extract arguments that are global for the batch of jobs to run
  job_resources = get_job_resources(args)
  job_metadata = get_job_metadata(args, script, provider)

  # Set up job parameters and job data from a tasks file or the command-line
  input_file_param_util = param_util.InputFileParamUtil(
      DEFAULT_INPUT_LOCAL_PATH)
  output_file_param_util = param_util.OutputFileParamUtil(
      DEFAULT_OUTPUT_LOCAL_PATH)
  if args.tasks:
    all_task_data = param_util.tasks_file_to_job_data(
        args.tasks, input_file_param_util, output_file_param_util)
  else:
    all_task_data = param_util.args_to_job_data(
        args.env, args.label, args.input, args.input_recursive, args.output,
        args.output_recursive, input_file_param_util, output_file_param_util)

  if not args.dry_run:
    print 'Job: %s' % job_metadata['job-id']

  # Wait for predecessor jobs (if any)
  if args.after:
    if args.dry_run:
      print '(Pretend) waiting for: %s.' % (args.after)
    else:
      print 'Waiting for predecessor jobs to complete...'
      error_messages = wait_after(provider, args.after, args.poll_interval,
                                  True)
      if error_messages:
        print('One or more predecessor jobs completed, but did not succeed. '
              'Exiting.')
        for msg in error_messages:
          print_error(msg)
        sys.exit(1)

  # If requested, skip running this job if its outputs already exist
  if args.skip and not args.dry_run:
    if _job_outputs_are_present(all_task_data[0]):
      print 'Job output already present, skipping new job submission.'
      return {'job-id': NO_JOB}

  # Launch all the job tasks!
  launched_job = provider.submit_job(job_resources, job_metadata, all_task_data)

  if not args.dry_run:
    print 'Launched job-id: %s' % launched_job['job-id']
    if launched_job.get('task-id'):
      print '%s task(s)' % len(launched_job['task-id'])
    print 'To check the status, run:'
    print '  dstat%s --jobs %s --status \'*\'' % (
        provider_base.get_dstat_provider_args(args), launched_job['job-id'])
    print 'To cancel the job, run:'
    print '  ddel%s --jobs %s' % (provider_base.get_ddel_provider_args(args),
                                  launched_job['job-id'])

  # Poll for job completion
  if args.wait:
    print 'Waiting for job to complete...'

    error_messages = wait_after(provider, [job_metadata['job-id']],
                                args.poll_interval, False)
    if error_messages:
      for msg in error_messages:
        print_error(msg)
      sys.exit(1)

  return launched_job


def _name_for_command(command):
  r"""Craft a simple command name from the command.

  The best command strings for this are going to be those where a simple
  command was given:

  >>> _name_for_command('samtools index "${BAM}"')
  'samtools'
  >>> _name_for_command('/usr/bin/sort "${INFILE}" > "${OUTFILE}"')
  'sort'

  For commands like "export VAR=val\necho ${VAR}", the user may want to pass
  --name to specify a more informative name.

  Arguments:
    command: the user-provided command
  Returns:
    a proposed name for the task.
  """

  # strip() to eliminate any leading whitespace from the token:
  return os.path.basename(re.split(r'\s', command.strip())[0])

if __name__ == '__main__':
  main(sys.argv[0], sys.argv[1:])
