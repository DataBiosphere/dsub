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

"""View dsub job and task status.

Follows the model of bjobs, sinfo, qstat, etc.
"""

import argparse
import collections
import time

from lib import dsub_util
from providers import provider_base

import tabulate

MAX_ERROR_MESSAGE_LENGTH = 30
MAX_INPUT_ARGS_LENGTH = 50


def parse_arguments():
  """Parses command line arguments.

  Returns:
    A Namespace of parsed arguments.
  """
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--project',
      required=True,
      help='Cloud project ID in which to query pipeline operations')
  parser.add_argument(
      '-j', '--jobs', nargs='*', help='A list of jobs on which to check status')
  parser.add_argument(
      '-u',
      '--users',
      nargs='*',
      default=[dsub_util.get_default_user()],
      help="""Lists only those jobs which were submitted by the list of users.
          Use "*" to list jobs of any user.""")
  parser.add_argument(
      '-s',
      '--status',
      nargs='*',
      default=['RUNNING'],
      choices=['RUNNING', 'SUCCESS', 'FAILURE', 'CANCELED', '*'],
      help="""Lists only those jobs which match the specified status(es).
          Use "*" to list jobs of any status.""")
  parser.add_argument(
      '--poll-interval',
      default=10,
      type=int,
      help='Polling interval (in seconds) for checking job status '
      'when --wait is set.')
  parser.add_argument(
      '--wait', action='store_true', help='Wait until jobs have all completed.')
  parser.add_argument(
      '-f',
      '--full',
      action='store_true',
      help='Toggle output with full operation identifiers'
      ' and input parameters.')
  return parser.parse_args()


def trim_display_field(value, length):
  if len(value) > length:
    return value[:length - 3] + '...'
  return value


def prepare_row(provider, job, full):
  """return ordered dict with the job's info (more if "full" is set)."""
  job_name = provider.get_job_field(job, 'job-name')
  job_id = provider.get_job_field(job, 'job-id')
  task_id = provider.get_job_field(job, 'task-id')
  status, last_update = provider.get_job_status_message(job)

  row = collections.OrderedDict()
  if job_name:
    row['Job Name'] = job_name
  if task_id:
    row['Task'] = task_id

  row['Status'] = trim_display_field(status, MAX_ERROR_MESSAGE_LENGTH)
  row['Last Update'] = last_update

  if full:
    create_time = provider.get_job_field(job, 'create-time')
    end_time = provider.get_job_field(job, 'end-time')
    inputs = provider.get_job_field(job, 'inputs')
    input_str = ','.join('%s=%s' % (key, value)
                         for key, value in inputs.iteritems())
    user_id = provider.get_job_field(job, 'user-id')
    internal_id = provider.get_job_field(job, 'internal-id')
    row['Created'] = create_time
    row['Ended'] = end_time if end_time else 'NA'
    row['User'] = user_id
    row['Job ID'] = job_id
    row['Internal ID'] = internal_id
    row['Inputs'] = trim_display_field(input_str, MAX_INPUT_ARGS_LENGTH)
  return row


def main():
  # Parse args and validate
  args = parse_arguments()

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Track if any jobs are running in the event --wait was requested.
  some_job_running = True
  while some_job_running:

    jobs = provider.get_jobs(
        args.status, user_list=args.users, job_list=args.jobs)

    # Try to keep the default behavior rational based on real usage patterns.
    # Most common usage:
    # * User kicked off one or more single-operation jobs, or
    # * User kicked off a single "array job".
    # * User just wants to check on the status of their own running jobs.
    #
    # qstat and hence dstat.py defaults to listing jobs for the current user, so
    # there is no need to include user information in the default output.

    # The information you want in that case is very different than other uses,
    # such as:
    # * I want to see all jobs currently pending/running
    # * I want to see status for a particular job or set of jobs
    #   (including the finished jobs)

    # Job name is typically short
    # Job ID is typically long
    # Task ID is short

    table = []

    some_job_running = False
    for job in jobs:
      table.append(prepare_row(provider, job, args.full))
      if provider.get_job_field(job, 'job-status') == 'RUNNING':
        some_job_running = True

    if table:
      print tabulate.tabulate(table, headers='keys')

    if args.wait and some_job_running:
      time.sleep(args.poll_interval)
    else:
      break

if __name__ == '__main__':
  main()
