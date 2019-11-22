# Lint as: python2, python3
# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility classes for outputting job and task information.

Used by dsub and dstat.
"""
from __future__ import print_function

import collections
import datetime
import json
from dateutil.tz import tzlocal

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

  def prepare_and_print_table(self, rows, summary):
    """Wrapper for prepare_output and print_table."""
    if summary:
      rows = prepare_summary_table(rows)

    table = []
    for row in rows:
      row = self.prepare_output(row)
      table.append(row)
    self.print_table(table)


class TextOutput(OutputFormatter):
  """Format output for text display."""

  _MAX_ERROR_MESSAGE_LENGTH = 30

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
        ('input-recursives', 'Input Recursives', self.format_pairs),
        ('outputs', 'Outputs', self.format_pairs),
        ('output-recursives', 'Output Recursives', self.format_pairs),
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
    return dumper.represent_dict(list(data.items()))

  def print_table(self, table):
    print(yaml.dump(table, default_flow_style=False))


class JsonOutput(OutputFormatter):
  """Format output for JSON display."""

  @classmethod
  def serialize(cls, field):
    if isinstance(field, datetime.datetime):
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


def prepare_summary_table(rows):
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


def prepare_row(task, full, summary):
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
      row_spec('input-recursives', False, {}),
      row_spec('outputs', True, {}),
      row_spec('output-recursives', False, {}),
      row_spec('mounts', True, {}),
      row_spec('provider', True, None),
      row_spec('provider-attributes', True, {}),
      row_spec('events', True, []),
      row_spec('user-project', False, None),
      row_spec('dsub-version', False, None),
      row_spec('script-name', False, None),
      row_spec('script', False, None),
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
