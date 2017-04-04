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
from contextlib import contextmanager
import os
import sys
import time

from lib import dsub_util
from lib import job_util
from lib import param_util
from lib.dsub_util import print_error
from providers import provider_base

SLEEP_FUNCTION = time.sleep  # so we can replace it in tests

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/bigquery',]

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
# Similar functionality is available in the header row of a TSV table file:
#
#   --input
#   --input VAR
#
#   --output
#   --output VAR

DEFAULT_INPUT_LOCAL_PATH = 'input'
DEFAULT_OUTPUT_LOCAL_PATH = 'output'


class Printer(object):
  """File-like stream object that redirects stdout to stderr.

  Args:
    object: parent object. Linter made me write this.
  """

  def __init__(self, args):
    self._args = args
    self._actual_stdout = sys.stdout

  def write(self, buf):
    sys.stderr.write(buf)


@contextmanager
def replace_print_with(printer):
  """Sys.out replacer.

  Use it like this:
  with replace_print_with(fileobj):
    print "hello"  # writes to the file
  print "done"  # prints to stdout

  Args:
    printer: a file-like object that will replace stdout.

  Yields:
    The printer.
  """
  previous_stdout = sys.stdout
  sys.stdout = printer
  try:
    yield printer
  finally:
    sys.stdout = previous_stdout


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


def parse_arguments(prog, argv):
  """Parses command line arguments.

  Args:
    prog: The path of the program (dsub.py) or an alternate program name to
    display in usage.
    argv: The list of program arguments to parse.

  Returns:
    A Namespace of parsed arguments.
  """
  parser = argparse.ArgumentParser(
      prog=prog, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--project',
      required=True,
      help='Cloud project ID in which to run the pipeline')
  parser.add_argument(
      '--logging',
      required=True,
      help='Cloud Storage path to send logging output')
  parser.add_argument(
      '--name',
      help='Name for pipeline. Defaults to the script name or'
      'first token of the --command if specified.')
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
      '--boot-disk-size',
      default=10,
      type=int,
      help='Size (in GB) of the boot disk')
  parser.add_argument(
      '--preemptible',
      default=False,
      action='store_true',
      help='Use a preemptible VM for the job')
  parser.add_argument(
      '--zones',
      default=None,
      nargs='+',
      help='List of Google Compute Engine zones.')
  parser.add_argument(
      '--table',
      default=None,
      help='Path to TSV of job parameters. Each column'
      ' specifies an environment variable to set in each'
      ' jobs\'s parent shell, and each row specifies the values'
      ' of those variables for each job.')
  parser.add_argument(
      '--image',
      default='ubuntu:14.04',
      help='Image name from Docker Hub, Google Container Repository, or other'
      ' Docker image service. The pipeline must have READ access to the image.')
  parser.add_argument(
      '--scopes',
      default=DEFAULT_SCOPES,
      nargs='+',
      help='Space-separated scopes for GCE instances.')
  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Print the pipeline(s) that would be run and then exit.')
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
      '--env',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Environment variables for the script\'s execution environment',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--input',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Input path arguments to localize into the script\'s execution'
      ' environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--input-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Input path arguments to localize recursively into the script\'s'
      ' execution environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Output path arguments to de-localize from the script\'s execution'
      ' environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Output path arguments to de-localize recursively from the script\'s'
      ' execution environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--after',
      nargs='+',
      default=[],
      help='Job ID(s) to wait for before starting this job.')
  parser.add_argument(
      '--command',
      help='Command to run inside the job\'s Docker container',
      metavar='COMMAND')
  parser.add_argument(
      '--script',
      help='Local path to a script to run inside the job\'s Docker container.',
      metavar='SCRIPT')
  parser.add_argument(
      'deprecated_script', nargs='?', help=argparse.SUPPRESS, metavar='SCRIPT')
  parser.add_argument(
      'params',
      nargs='*',
      default=[],
      help=argparse.SUPPRESS,
      metavar='KEY=VALUE')
  return parser.parse_args(argv)


def get_job_resources(args):
  """Extract job-global resources requirements from input args.

  Args:
    args: parsed command-line arguments

  Returns:
    JobResources object containing the requested resources for the job
  """

  return job_util.JobResources(
      min_cores=args.min_cores,
      min_ram=args.min_ram,
      disk_size=args.disk_size,
      boot_disk_size=args.boot_disk_size,
      preemptible=args.preemptible,
      image=args.image,
      zones=args.zones,
      logging=args.logging,
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

  job_metadata = provider.get_job_metadata(script.name, args.name,
                                           dsub_util.get_default_user(),
                                           args.table is not None)
  job_metadata['script'] = script

  return job_metadata


def tasks_to_job_ids(provider, task_list):
  """Returns the set of job IDs for the given tasks."""
  return set([provider.get_job_field(t, 'job-id') for t in task_list])


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
  job_set = set(jobid_list)
  error_messages = []
  while job_set and (not error_messages or not stop_on_failure):
    print 'Waiting for: %s.' % (', '.join(job_set))

    jobs_left = wait_for_any_job(provider, job_set, poll_interval)
    jobs_completed = job_set.difference(jobs_left)
    tasks_completed = provider.get_jobs(
        ['RUNNING', 'SUCCESS', 'FAILURE', 'CANCELED'],
        job_list=jobs_completed,
        max_jobs=len(jobs_completed))
    if len(tasks_completed) != len(jobs_completed):
      # print info about the jobs we couldn't find
      # (likely a typo in the command line).
      jobs_found = tasks_to_job_ids(provider, tasks_completed)
      jobs_not_found = jobs_completed.difference(jobs_found)
      for j in jobs_not_found:
        error = '%s: not found' % (j)
        print_error('  %s' % (error))
        error_messages += [error]

    for t in tasks_completed:
      job_id = provider.get_job_field(t, 'job-id')
      status = provider.get_job_status_message(t)
      print '  %s: %s' % (str(job_id), str(status))
      if status[0] in ['FAILURE', 'CANCELED']:
        error_messages += [provider.get_job_completion_messages([t])]

    job_set = jobs_left

  return error_messages


def wait_for_any_job(provider, jobid_list, poll_interval):
  """Waits until any of the listed jobs is not running.

  Args:
    provider: job service provider
    jobid_list: a list of job IDs (string) to wait for
    poll_interval: integer seconds to wait between iterations

  Returns:
    A set of the jobIDs with still at least one running task.
  """
  while True:
    running_jobs = tasks_to_job_ids(provider,
                                    provider.get_jobs(
                                        ['RUNNING'], job_list=jobid_list))
    if len(running_jobs) != len(jobid_list):
      return running_jobs
    SLEEP_FUNCTION(poll_interval)


def call(argv):
  return main('%s.call' % __name__, argv)


def main(prog, argv):
  # Parse args and validate
  args = parse_arguments(prog, argv)
  # intent:
  # * dsub tightly controls the output to stdout.
  # * wrap the main body such that output goes to stderr.
  # * only emit the job-id to stdout (which can then be used programmatically).
  with replace_print_with(Printer(args)):
    launched_job = run_main(args)
  print launched_job.get('job-id', '')
  return launched_job


def run_main(args):
  """Actual dsub body, post-stdout-redirection."""
  if args.command and args.script:
    raise ValueError('Cannot supply both --command and a script name')

  if args.deprecated_script:
    print_error('Using a positional argument for the job script is '
                'deprecated.')
    print_error('Use the --script argument instead.')

    # Set the script from the deprecated positional argument.
    args.script = args.deprecated_script

  if args.params:
    print_error('Using positional arguments for input variables is '
                'deprecated.')
    print_error('Use the --env argument instead.')

    # Merge args.params into args.env
    args.env.extend(args.params)

  if (args.env or args.input or args.input_recursive or args.output or
      args.output_recursive) and args.table:
    raise ValueError('Cannot supply both command-line parameters '
                     '(--env/--input/--input-recursive/--output/'
                     '--output-recursive) and --table')

  if args.command:
    if args.name:
      command_name = args.name
    else:
      command_name = args.command.split(' ', 1)[0]
    script = job_util.Script(command_name, args.command)
  elif args.script:
    # Read the script file
    with open(args.script, 'r') as script_file:
      script = job_util.Script(
          os.path.basename(args.script), script_file.read())
  else:
    raise ValueError('One of --command or a script name must be supplied')

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Extract arguments that are global for the batch of jobs to run
  job_resources = get_job_resources(args)
  job_metadata = get_job_metadata(args, script, provider)

  # Set up job parameters and job data from a table file or the command-line
  input_file_param_util = param_util.InputFileParamUtil(
      DEFAULT_INPUT_LOCAL_PATH)
  output_file_param_util = param_util.OutputFileParamUtil(
      DEFAULT_OUTPUT_LOCAL_PATH)
  if args.table:
    all_job_data = param_util.table_to_job_data(
        args.table, input_file_param_util, output_file_param_util)
  else:
    all_job_data = param_util.args_to_job_data(
        args.env, args.input, args.input_recursive, args.output,
        args.output_recursive, input_file_param_util, output_file_param_util)

  if not args.dry_run:
    print 'Job: %s' % job_metadata['job-id']

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

  # Launch all the job tasks!
  launched_job = provider.submit_job(job_resources, job_metadata, all_job_data)

  if not args.dry_run:
    print 'Launched job-id: %s' % launched_job['job-id']
    if launched_job.get('task-id'):
      print '%s task(s)' % len(launched_job['task-id'])
    print 'To check the status, run:'
    print '  dstat --project %s --jobs %s' % (args.project,
                                              launched_job['job-id'])
    print 'To cancel the job, run:'
    print '  ddel --project %s --jobs %s' % (args.project,
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


if __name__ == '__main__':
  main(sys.argv[0], sys.argv[1:])
