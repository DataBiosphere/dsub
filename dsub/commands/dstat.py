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


import argparse
import collections
import json
import time

from ..lib import dsub_util
from ..providers import provider_base

import tabulate
import yaml


class OutputFormatter(object):
  """Base class for supported output formats."""

  def __init__(self, full):
    self._full = full

  def prepare_output(self, row):
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

  def prepare_output(self, row):

    # Define the ordering of fields for text output along with any
    # transformations.
    column_map = [
        ('job-id', 'Job ID',),
        ('job-name', 'Job Name',),
        ('task-id', 'Task',),
        ('status', 'Status', self.format_status),
        ('last-update', 'Last Update',),
        ('create-time', 'Created',),
        ('end-time', 'Ended',),
        ('user-id', 'User',),
        ('internal-id', 'Internal ID',),
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
    print tabulate.tabulate(table, headers='keys')
    print


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
    print yaml.dump(table, default_flow_style=False)


class JsonOutput(OutputFormatter):
  """Format output for JSON display."""

  def __init__(self, full):
    super(JsonOutput, self).__init__(full)

  def print_table(self, table):
    print json.dumps(table, indent=2)


def prepare_row(provider, task, full):
  """return a dict with the task's info (more if "full" is set)."""

  # Would like to include the Job ID in the default set of columns, but
  # it is a long value and would leave little room for status and update time.

  # pyformat: disable
  default_columns = [
      ('job-name', False),
      ('task-id', True),
      ('status', False),
      ('last-update', False)
  ]
  full_columns = [
      ('job-id',),
      ('user-id',),
      ('create-time',),
      ('end-time', 'NA'),
      ('internal-id',),
      ('inputs', {}),
      ('outputs', {}),
  ]
  # pyformat: enable

  row = {}
  for col in default_columns:
    key = col[0]
    optional = col[1]

    value = provider.get_task_field(task, key)
    if not optional or value:
      row[key] = value

  if full:
    for col in full_columns:
      key = col[0]
      default = col[1] if len(col) > 1 else None

      row[key] = provider.get_task_field(task, key, default)

  return row


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
      '--limit',
      type=int,
      help='The maximum number of tasks to list. The default is unlimited.')
  parser.add_argument(
      '-f',
      '--full',
      action='store_true',
      help='Toggle output with full operation identifiers'
      ' and input parameters.')
  parser.add_argument(
      '--format',
      choices=['text', 'json', 'yaml'],
      help='Set the output format.')
  return parser.parse_args()


def main():

  # Parse args and validate
  args = parse_arguments()

  if args.format == 'json':
    output_formatter = JsonOutput(args.full)
  elif args.format == 'text':
    output_formatter = TextOutput(args.full)
  elif args.format == 'yaml':
    output_formatter = YamlOutput(args.full)
  else:
    # If --full is passed, then format defaults to yaml.
    # Else format defaults to text
    if args.full:
      output_formatter = YamlOutput(args.full)
    else:
      output_formatter = TextOutput(args.full)

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Track if any jobs are running in the event --wait was requested.
  some_job_running = True
  while some_job_running:

    tasks = provider.lookup_job_tasks(
        args.status,
        user_list=args.users,
        job_list=args.jobs,
        max_jobs=args.limit)

    table = []

    some_job_running = False
    for task in tasks:
      row = prepare_row(provider, task, args.full)
      row = output_formatter.prepare_output(row)

      table.append(row)
      if provider.get_task_field(task, 'job-status') == 'RUNNING':
        some_job_running = True

    if table:
      output_formatter.print_table(table)

    if args.wait and some_job_running:
      time.sleep(args.poll_interval)
    else:
      break

if __name__ == '__main__':
  main()
