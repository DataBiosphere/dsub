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
from ..lib import job_model
from ..lib import param_util
from ..lib import resources
from ..providers import provider_base

import six
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
    """Convert types of task fields."""
    date_fields = ['last-update', 'create-time', 'start-time', 'end-time']
    int_fields = ['task-attempt']

    for col in date_fields:
      if col in row:
        row[col] = self.default_format_date(row[col])

    for col in int_fields:
      if col in row and row[col] is not None:
        row[col] = int(row[col])

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

  def format_pairs(self, values):
    """Returns a string of comma-delimited key=value pairs."""
    return ', '.join(
        '%s=%s' % (key, value) for key, value in sorted(values.items()))

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
        ('task-attempt', 'Attempt'),
        ('status-message', 'Status', self.format_status),
        ('status-detail', 'Status Details'),
        ('last-update', 'Last Update', self.text_format_date),
        ('create-time', 'Created', self.text_format_date),
        ('start-time', 'Started', self.text_format_date),
        ('end-time', 'Ended', self.text_format_date),
        ('user-id', 'User'),
        ('internal-id', 'Internal ID'),
        ('logging', 'Logging'),
        ('labels', 'Labels', self.format_pairs),
        ('envs', 'Environment Variables', self.format_pairs),
        ('inputs', 'Inputs', self.format_pairs),
        ('outputs', 'Outputs', self.format_pairs),
        ('mounts', 'Mounts', self.format_pairs),
        ('user-project', 'User Project'),
        ('dsub-version', 'Version'),
        # These fields only shows up when summarizing
        ('status', 'Status'),
        ('task-count', 'Task Count'),
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
    if not table:
      # Old versions of tabulate (0.7.5)  emit 'k e y s\n--- --- --- ---'
      print('')
    else:
      print(tabulate.tabulate(table, headers='keys'))
    print('')


class YamlOutput(OutputFormatter):
  """Format output for YAML display."""

  def __init__(self, full):
    super(YamlOutput, self).__init__(full)

    yaml.add_representer(six.text_type, self.string_presenter)
    yaml.add_representer(str, self.string_presenter)
    yaml.add_representer(collections.OrderedDict, self.dict_representer)

  def string_presenter(self, dumper, data):
    """Presenter to force yaml.dump to use multi-line string style."""
    if '\n' in data:
      return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    else:
      return dumper.represent_scalar('tag:yaml.org,2002:str', data)

  def dict_representer(self, dumper, data):
    return dumper.represent_dict(data.items())

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
    # Prior to Python 3.4, json.dumps() with an indent included
    # trailing whitespace (see https://bugs.python.org/issue16333).
    separators_to_eliminate_trailing_whitespace = (',', ': ')
    print(
        json.dumps(
            table,
            indent=2,
            default=self.serialize,
            separators=separators_to_eliminate_trailing_whitespace))


def _prepare_summary_table(rows):
  """Create a new table that is a summary of the input rows.

  All with the same (job-name or job-id, status) go together.

  Args:
    rows: the input rows, a list of dictionaries.
  Returns:
    A new row set of summary information.
  """
  if not rows:
    return []

  # We either group on the job-name (if present) or fall back to the job-id
  key_field = 'job-name'
  if key_field not in rows[0]:
    key_field = 'job-id'

  # Group each of the rows based on (job-name or job-id, status)
  grouped = collections.defaultdict(lambda: collections.defaultdict(lambda: []))
  for row in rows:
    grouped[row.get(key_field, '')][row.get('status', '')] += [row]

  # Now that we have the rows grouped, create a summary table.
  # Use the original table as the driver in order to preserve the order.
  new_rows = []
  for job_key in sorted(grouped.keys()):
    group = grouped.get(job_key, None)
    canonical_status = ['RUNNING', 'SUCCESS', 'FAILURE', 'CANCEL']
    # Written this way to ensure that if somehow a new status is introduced,
    # it shows up in our output.
    for status in canonical_status + sorted(group.keys()):
      if status not in group:
        continue
      task_count = len(group[status])
      del group[status]
      if task_count:
        summary_row = collections.OrderedDict()
        summary_row[key_field] = job_key
        summary_row['status'] = status
        summary_row['task-count'] = task_count
        new_rows.append(summary_row)

  return new_rows


def _prepare_row(task, full, summary):
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
      row_spec('task-attempt', False, None),
      row_spec('create-time', True, None),
      row_spec('start-time', True, None),
      row_spec('end-time', True, None),
      row_spec('internal-id', True, None),
      row_spec('logging', True, None),
      row_spec('labels', True, {}),
      row_spec('envs', True, {}),
      row_spec('inputs', True, {}),
      row_spec('outputs', True, {}),
      row_spec('mounts', True, {}),
      row_spec('provider', True, None),
      row_spec('provider-attributes', True, {}),
      row_spec('events', True, []),
      row_spec('user-project', False, None),
      row_spec('dsub-version', False, None),
  ]
  summary_columns = default_columns + [
      row_spec('job-id', True, None),
      row_spec('user-id', True, None),
      row_spec('status', True, None),
  ]
  # pyformat: enable

  assert not (full and summary), 'Full and summary cannot both be enabled'

  if full:
    columns = full_columns
  elif summary:
    columns = summary_columns
  else:
    columns = default_columns

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
  # Shared arguments between the "google" and "google-v2" providers
  google_common = parser.add_argument_group(
      title='google-common',
      description='Options common to the "google" and "google-v2" providers')
  google_common.add_argument(
      '--project',
      help='Cloud project ID in which to find and delete the job(s)')

  return provider_base.parse_args(
      parser, {
          'google': ['project'],
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
    if args.summary:
      rows = _prepare_summary_table(rows)

    table = []
    for row in rows:
      row = output_formatter.prepare_output(row)
      table.append(row)
    output_formatter.print_table(table)


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
        page_size=max_tasks)

    some_job_running = False

    formatted_tasks = []
    for task in tasks:
      if 0 < max_tasks <= len(formatted_tasks):
        break

      # Format tasks as specified.
      if raw_format:
        formatted_tasks.append(task.raw_task_data())
      else:
        formatted_tasks.append(_prepare_row(task, full_output, summary_output))

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
    yield _prepare_row(task, True, summary_output)


if __name__ == '__main__':
  main()
