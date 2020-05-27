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

"""View dsub job and task status.

Follows the model of bjobs, sinfo, qstat, etc.
"""

# Try to keep the default behavior rational based on real usage patterns.
# Most common usage:
# * User kicked off one or more single-operation jobs, or
# * User kicked off a single "array job".
# * User just wants to check on the status of their own running jobs.
#
# qstat and hence dstat.py defaults to listing jobs for the current user, so
# there is no need to include user information in the default output.

from __future__ import print_function

import sys
import time

from ..lib import dsub_util
from ..lib import job_model
from ..lib import output_formatter
from ..lib import param_util
from ..lib import resources
from ..providers import provider_base


def _parse_arguments():
  """Parses command line arguments.

  Returns:
    A Namespace of parsed arguments.
  """
  # Handle version flag and exit if it was passed.
  param_util.handle_version_flag()

  parser = provider_base.create_parser(sys.argv[0])

  parser.add_argument(
      '--version', '-v', default=False, help='Print the dsub version and exit.')

  parser.add_argument(
      '--jobs',
      '-j',
      nargs='*',
      help='A list of jobs IDs on which to check status')
  parser.add_argument(
      '--names',
      '-n',
      nargs='*',
      help='A list of job names on which to check status')
  parser.add_argument(
      '--tasks',
      '-t',
      nargs='*',
      help='A list of task IDs on which to check status')
  parser.add_argument(
      '--attempts',
      nargs='*',
      help='A list of task attempts on which to check status')
  parser.add_argument(
      '--users',
      '-u',
      nargs='*',
      default=[],
      help="""Lists only those jobs which were submitted by the list of users.
          Use "*" to list jobs of any user.""")
  parser.add_argument(
      '--status',
      '-s',
      nargs='*',
      default=['RUNNING'],
      choices=['RUNNING', 'SUCCESS', 'FAILURE', 'CANCELED', '*'],
      help="""Lists only those jobs which match the specified status(es).
          Choose from {'RUNNING', 'SUCCESS', 'FAILURE', 'CANCELED'}.
          Use "*" to list jobs of any status.""",
      metavar='STATUS')
  parser.add_argument(
      '--age',
      help="""List only those jobs newer than the specified age. Ages can be
          listed using a number followed by a unit. Supported units are
          s (seconds), m (minutes), h (hours), d (days), w (weeks).
          For example: '7d' (7 days). Bare numbers are treated as UTC.""")
  parser.add_argument(
      '--label',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help='User labels to match. Tasks returned must match all labels.',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--poll-interval',
      default=10,
      type=int,
      help='Polling interval (in seconds) for checking job status '
      'when --wait is set.')
  parser.add_argument(
      '--wait', action='store_true', help='Wait until jobs have all completed.')
  parser.add_argument(
      '--limit',
      default=0,
      type=int,
      help='The maximum number of tasks to list. The default is unlimited.')
  parser.add_argument(
      '--format',
      choices=['text', 'json', 'yaml', 'provider-json'],
      help='Set the output format.')
  output_style = parser.add_mutually_exclusive_group()
  output_style.add_argument(
      '--full',
      '-f',
      action='store_true',
      help='Display output with full task information'
      ' and input parameters.')
  output_style.add_argument(
      '--summary',
      action='store_true',
      help='Display a summary of the results, grouped by (job, status).')

  # Shared between the "google-cls-v2" and "google-v2" providers
  google_common = parser.add_argument_group(
      title='google-common',
      description="""Options common to the "google", "google-cls-v2", and
        "google-v2" providers""")
  google_common.add_argument(
      '--project', help='Cloud project ID in which to find and the job(s)')

  google_cls_v2 = parser.add_argument_group(
      title='"google-cls-v2" provider options',
      description='See also the "google-common" options listed')
  google_cls_v2.add_argument(
      '--location',
      default=job_model.DEFAULT_LOCATION,
      help="""Specifies the Google Cloud region to which the dsub job was
        submitted. (default: {})""".format(job_model.DEFAULT_LOCATION))

  return provider_base.parse_args(
      parser, {
          'google-cls-v2': ['project'],
          'google-v2': ['project'],
          'test-fails': [],
          'local': [],
      }, sys.argv[1:])


def main():
  # Parse args and validate
  args = _parse_arguments()

  # Compute the age filter (if any)
  create_time_min = param_util.age_to_create_time(args.age)

  # Set up the output formatter
  if args.format == 'json':
    formatter = output_formatter.JsonOutput(args.full)
  elif args.format == 'text':
    formatter = output_formatter.TextOutput(args.full)
  elif args.format == 'yaml':
    formatter = output_formatter.YamlOutput(args.full)
  elif args.format == 'provider-json':
    formatter = output_formatter.JsonOutput(args.full)
  else:
    # If --full is passed, then format defaults to yaml.
    # Else format defaults to text
    if args.full:
      formatter = output_formatter.YamlOutput(args.full)
    else:
      formatter = output_formatter.TextOutput(args.full)

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args, resources)
  with dsub_util.replace_print():
    provider_base.emit_provider_message(provider)

  # Set poll interval to zero if --wait is not set.
  poll_interval = args.poll_interval if args.wait else 0

  # Make sure users were provided, or try to fill from OS user. This cannot
  # be made into a default argument since some environments lack the ability
  # to provide a username automatically.
  user_ids = set(args.users) if args.users else {dsub_util.get_os_user()}
  labels = param_util.parse_pair_args(args.label, job_model.LabelParam)

  job_producer = dstat_job_producer(
      provider=provider,
      statuses=set(args.status) if args.status else None,
      user_ids=user_ids,
      job_ids=set(args.jobs) if args.jobs else None,
      job_names=set(args.names) if args.names else None,
      task_ids=set(args.tasks) if args.tasks else None,
      task_attempts=set(args.attempts) if args.attempts else None,
      labels=labels if labels else None,
      create_time_min=create_time_min,
      max_tasks=args.limit,
      full_output=args.full,
      summary_output=args.summary,
      poll_interval=poll_interval,
      raw_format=bool(args.format == 'provider-json'))

  # Track if any jobs are running in the event --wait was requested.
  for poll_event_tasks in job_producer:
    rows = poll_event_tasks
    formatter.prepare_and_print_table(rows, args.summary)


def dstat_job_producer(provider,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       task_attempts=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0,
                       full_output=False,
                       summary_output=False,
                       poll_interval=0,
                       raw_format=False):
  """Generate jobs as lists of task dicts ready for formatting/output.

  Args:
    provider: an instantiated dsub provider.
    statuses: a set of status strings that eligible jobs may match.
    user_ids: a set of user strings that eligible jobs may match.
    job_ids: a set of job-id strings eligible jobs may match.
    job_names: a set of job-name strings eligible jobs may match.
    task_ids: a set of task-id strings eligible tasks may match.
    task_attempts: a set of task-attempt strings eligible tasks may match.
    labels: set of LabelParam that all tasks must match.
    create_time_min: a timezone-aware datetime value for the earliest create
                     time of a task, inclusive.
    create_time_max: a timezone-aware datetime value for the most recent create
                     time of a task, inclusive.
    max_tasks: (int) maximum number of tasks to return per dstat job lookup.
    full_output: (bool) return all dsub fields.
    summary_output: (bool) return a summary of the job list.
    poll_interval: (int) wait time between poll events, dstat will poll jobs
                   until all jobs succeed or fail. Set to zero to disable
                   polling and return after the first lookup.
    raw_format: (bool) set True to prevent dsub from normalizing the task dict,
                this defaults to False and should only be set True if a
                provider-specific view of tasks is absolutely required.
                (NB: provider interfaces change over time, no transition path
                will be provided for users depending on this flag).

  Yields:
    lists of task dictionaries - each list representing a dstat poll event.
  """
  some_job_running = True
  while some_job_running:
    # Get a batch of jobs.
    tasks = provider.lookup_job_tasks(
        statuses,
        user_ids=user_ids,
        job_ids=job_ids,
        job_names=job_names,
        task_ids=task_ids,
        task_attempts=task_attempts,
        labels=labels,
        create_time_min=create_time_min,
        create_time_max=create_time_max,
        max_tasks=max_tasks,
        page_size=max_tasks,
        verbose=(poll_interval == 0))

    some_job_running = False

    formatted_tasks = []
    for task in tasks:
      if 0 < max_tasks <= len(formatted_tasks):
        break

      # Format tasks as specified.
      if raw_format:
        formatted_tasks.append(task.raw_task_data())
      else:
        formatted_tasks.append(
            output_formatter.prepare_row(task, full_output, summary_output))

      # Determine if any of the jobs are running.
      if task.get_field('task-status') == 'RUNNING':
        some_job_running = True

    # Yield the tasks and determine if the loop should continue.
    yield formatted_tasks
    if poll_interval and some_job_running:
      time.sleep(poll_interval)
    else:
      break


def lookup_job_tasks(provider,
                     statuses,
                     user_ids=None,
                     job_ids=None,
                     job_names=None,
                     task_ids=None,
                     task_attempts=None,
                     labels=None,
                     create_time_min=None,
                     create_time_max=None,
                     max_tasks=0,
                     page_size=0,
                     summary_output=False):
  """Generate formatted jobs individually, in order of create-time.

  Args:
    provider: an instantiated dsub provider.
    statuses: a set of status strings that eligible jobs may match.
    user_ids: a set of user strings that eligible jobs may match.
    job_ids: a set of job-id strings eligible jobs may match.
    job_names: a set of job-name strings eligible jobs may match.
    task_ids: a set of task-id strings eligible tasks may match.
    task_attempts: a set of task-attempt strings eligible tasks may match.
    labels: set of LabelParam that all tasks must match.
    create_time_min: a timezone-aware datetime value for the earliest create
                     time of a task, inclusive.
    create_time_max: a timezone-aware datetime value for the most recent create
                     time of a task, inclusive.
    max_tasks: (int) maximum number of tasks to return per dstat job lookup.
    page_size: the page size to use for each query to the backend. May be
               ignored by some provider implementations.
    summary_output: (bool) summarize the job list.

  Yields:
    Individual task dictionaries with associated metadata
  """
  tasks_generator = provider.lookup_job_tasks(
      statuses,
      user_ids=user_ids,
      job_ids=job_ids,
      job_names=job_names,
      task_ids=task_ids,
      task_attempts=task_attempts,
      labels=labels,
      create_time_min=create_time_min,
      create_time_max=create_time_max,
      max_tasks=max_tasks,
      page_size=page_size)

  # Yield formatted tasks.
  for task in tasks_generator:
    yield output_formatter.prepare_row(task, True, summary_output)


if __name__ == '__main__':
  main()
