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

import collections
from datetime import datetime
import json
import sys
import time
from dateutil.tz import tzlocal

from ..lib import dsub_util
from ..lib import param_util
from ..lib import resources
from ..providers import provider_base

import tabulate
import yaml


class OutputFormatter(object):
  """Base class for supported output formats."""

  def __init__(self, full):
    self._full = full

  def _format_date(self, dt, fmt):
    if not dt:
      return ''

    # Format dates using local timezone
    if dt.tzinfo:
      return dt.astimezone(tzlocal()).strftime(fmt)
    else:
      return dt.strftime(fmt)

  def format_date_micro(self, dt):
    return self._format_date(dt, '%Y-%m-%d %H:%M:%S.%f')

  def format_date_seconds(self, dt):
    return self._format_date(dt, '%Y-%m-%d %H:%M:%S')

  def default_format_date(self, dt):
    return self.format_date_micro(dt)

  def prepare_output(self, row):
    date_fields = ['last-update', 'create-time', 'start-time', 'end-time']

    for col in date_fields:
      if col in row:
        row[col] = self.default_format_date(row[col])

    return row

  def print_table(self, table):
    """Function to be defined by the derived class to print output."""
    raise NotImplementedError('print_table method not defined!')


class TextOutput(OutputFormatter):
  """Format output for text display."""

  _MAX_ERROR_MESSAGE_LENGTH = 30

  def __init__(self, full):
    super(TextOutput, self).__init__(full)

  def trim_display_field(self, value, max_length):
    """Return a value for display; if longer than max length, use ellipsis."""
    if not value:
      return ''
    if len(value) > max_length:
      return value[:max_length - 3] + '...'
    return value

  def format_status(self, status):
    if self._full:
      return status
    return self.trim_display_field(status, self._MAX_ERROR_MESSAGE_LENGTH)

  def format_inputs_outputs(self, values):
    """Returns a string of comma-delimited key=value pairs."""
    return ', '.join('%s=%s' % (key, value)
                     for key, value in sorted(values.iteritems()))

  def text_format_date(self, dt):
    if self._full:
      return self.format_date_micro(dt)
    return self.format_date_seconds(dt)

  def prepare_output(self, row):

    # Define the ordering of fields for text output along with any
    # transformations.
    column_map = [
        ('job-id', 'Job ID'),
        ('job-name', 'Job Name'),
        ('task-id', 'Task'),
        ('status-message', 'Status', self.format_status),
        ('status-detail', 'Status Details'),
        ('last-update', 'Last Update', self.text_format_date),
        ('create-time', 'Created', self.text_format_date),
        ('start-time', 'Started', self.text_format_date),
        ('end-time', 'Ended', self.text_format_date),
        ('user-id', 'User'),
        ('internal-id', 'Internal ID'),
        ('logging', 'Logging'),
        ('inputs', 'Inputs', self.format_inputs_outputs),
        ('outputs', 'Outputs', self.format_inputs_outputs),
    ]

    new_row = collections.OrderedDict()
    for col in column_map:
      field_name = col[0]
      if field_name not in row:
        continue

      text_label = col[1]

      if len(col) == 2:
        new_row[text_label] = row[field_name]
      else:
        format_fn = col[2]
        new_row[text_label] = format_fn(row[field_name])

    return new_row

  def print_table(self, table):
    print(tabulate.tabulate(table, headers='keys'))
    print('')


class YamlOutput(OutputFormatter):
  """Format output for YAML display."""

  def __init__(self, full):
    super(YamlOutput, self).__init__(full)

    yaml.add_representer(unicode, self.string_presenter)
    yaml.add_representer(str, self.string_presenter)

  def string_presenter(self, dumper, data):
    """Presenter to force yaml.dump to use multi-line string style."""
    if '\n' in data:
      return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    else:
      return dumper.represent_scalar('tag:yaml.org,2002:str', data)

  def print_table(self, table):
    print(yaml.dump(table, default_flow_style=False))


class JsonOutput(OutputFormatter):
  """Format output for JSON display."""

  def __init__(self, full):
    super(JsonOutput, self).__init__(full)

  @classmethod
  def serialize(cls, field):
    if isinstance(field, datetime):
      return str(field)
    return field

  def print_table(self, table):
    print(json.dumps(table, indent=2, default=self.serialize))


def _prepare_row(task, full):
  """return a dict with the task's info (more if "full" is set)."""

  # Would like to include the Job ID in the default set of columns, but
  # it is a long value and would leave little room for status and update time.

  row_spec = collections.namedtuple('row_spec',
                                    ['key', 'required', 'default_value'])

  # pyformat: disable
  default_columns = [
      row_spec('job-name', True, None),
      row_spec('task-id', False, None),
      row_spec('last-update', True, None),
      row_spec('status-message', True, None)
  ]
  full_columns = default_columns + [
      row_spec('job-id', True, None),
      row_spec('user-id', True, None),
      row_spec('status', True, None),
      row_spec('status-detail', True, None),
      row_spec('create-time', True, None),
      row_spec('start-time', True, None),
      row_spec('end-time', True, None),
      row_spec('internal-id', True, None),
      row_spec('logging', True, None),
      row_spec('inputs', True, {}),
      row_spec('outputs', True, {}),
      row_spec('envs', True, {}),
      row_spec('labels', True, {}),
  ]
  # pyformat: enable

  columns = full_columns if full else default_columns

  row = {}
  for col in columns:
    key, required, default = col

    value = task.get_field(key, default)
    if required or value is not None:
      row[key] = value

  return row


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
      type=int,
      help='The maximum number of tasks to list. The default is unlimited.')
  parser.add_argument(
      '--full',
      '-f',
      action='store_true',
      help='Toggle output with full operation identifiers'
      ' and input parameters.')
  parser.add_argument(
      '--format',
      choices=['text', 'json', 'yaml', 'provider-json'],
      help='Set the output format.')

  # Add provider-specific arguments
  google = parser.add_argument_group(
      title='google',
      description='Options for the Google provider (Pipelines API)')
  google.add_argument(
      '--project',
      help='Cloud project ID in which to find and delete the job(s)')

  return provider_base.parse_args(parser, {
      'google': ['project'],
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
    output_formatter = JsonOutput(args.full)
  elif args.format == 'text':
    output_formatter = TextOutput(args.full)
  elif args.format == 'yaml':
    output_formatter = YamlOutput(args.full)
  elif args.format == 'provider-json':
    output_formatter = JsonOutput(args.full)
  else:
    # If --full is passed, then format defaults to yaml.
    # Else format defaults to text
    if args.full:
      output_formatter = YamlOutput(args.full)
    else:
      output_formatter = TextOutput(args.full)

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args, resources)

  # Set poll interval to zero if --wait is not set.
  poll_interval = args.poll_interval if args.wait else 0

  # Make sure users were provided, or try to fill from OS user. This cannot
  # be made into a default argument since some environments lack the ability
  # to provide a username automatically.
  user_ids = set(args.users) if args.users else {dsub_util.get_os_user()}
  labels = param_util.parse_pair_args(args.label, param_util.LabelParam)

  job_producer = dstat_job_producer(
      provider=provider,
      statuses=set(args.status) if args.status else None,
      user_ids=user_ids,
      job_ids=set(args.jobs) if args.jobs else None,
      job_names=set(args.names) if args.names else None,
      task_ids=set(args.tasks) if args.tasks else None,
      labels=labels if labels else None,
      create_time_min=create_time_min,
      max_tasks=args.limit,
      full_output=args.full,
      poll_interval=poll_interval,
      raw_format=bool(args.format == 'provider-json'))

  # Track if any jobs are running in the event --wait was requested.
  for poll_event_tasks in job_producer:
    table = []
    for row in poll_event_tasks:
      row = output_formatter.prepare_output(row)
      table.append(row)
    output_formatter.print_table(table)


def dstat_job_producer(provider,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0,
                       full_output=False,
                       poll_interval=0,
                       raw_format=False):
  """Generate jobs as lists of task dicts ready for formatting/output.

  This function separates dstat logic from flag parsing and user output. Users
  of dstat who intend to access the data programmatically should use this.

  Args:
    provider: an instantiated dsub provider.
    statuses: a set of status strings that eligible jobs may match.
    user_ids: a set of user strings that eligible jobs may match.
    job_ids: a set of job-id strings eligible jobs may match.
    job_names: a set of job-name strings eligible jobs may match.
    task_ids: a set of task-id strings eligible tasks may match.
    labels: set of LabelParam that all tasks must match.
    create_time_min: a timezone-aware datetime value for the earliest create
                     time of a task, inclusive.
    create_time_max: a timezone-aware datetime value for the most recent create
                     time of a task, inclusive.
    max_tasks: (int) maximum number of tasks to return per dstat job lookup.
    full_output: (bool) return all dsub fields.
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
        labels=labels,
        create_time_min=create_time_min,
        create_time_max=create_time_max,
        max_tasks=max_tasks)

    some_job_running = False

    formatted_tasks = []
    for task in tasks:
      # Format tasks as specified.
      if raw_format:
        formatted_tasks.append(task.raw_task_data())
      else:
        formatted_tasks.append(_prepare_row(task, full_output))

      # Determine if any of the jobs are running.
      if task.get_field('task-status') == 'RUNNING':
        some_job_running = True

    # Yield the tasks and determine if the loop should continue.
    yield formatted_tasks
    if poll_interval and some_job_running:
      time.sleep(poll_interval)
    else:
      break

if __name__ == '__main__':
  main()
