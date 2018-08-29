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

"""Utility functions for python tests of dsub."""

import difflib
import re
import subprocess
import sys


def to_string(stdoutbytes):
  """Convert stdout to a string in python 2 or 3."""
  if sys.version_info[0] < 3:
    return stdoutbytes
  # Get the stdout system encoding. If unknown, assume utf-8.
  encoding = sys.stdout.encoding if sys.stdout.encoding else 'utf-8'
  return stdoutbytes.decode(encoding)


def gsutil_ls_check(path):
  return not subprocess.call('gsutil ls "%s" 2>/dev/null' % path, shell=True)


def gsutil_cat(path):
  return to_string(
      subprocess.check_output('gsutil cat "%s"' % path, shell=True))


def diff(str1, str2):
  """Determine if two strings differ, emit differences to stdout."""
  result = list(difflib.Differ().compare(
      str1.splitlines(True), str2.splitlines(True)))

  same = True
  for line in result:
    if line[0] in ('-', '+'):
      same = False
      sys.stdout.write(line)

  return same


def expand_tsv_fields(newenv, tmpl_file, tsv_file):
  """Write a TSV file, expanding environment variables in the template."""
  with open(tmpl_file, 'r') as f:
    input_lines = f.readlines()

  with open(tsv_file, 'w') as f:
    # Emit the header line unchanged
    f.write(input_lines[0])

    for line in input_lines[1:]:
      curr = []
      for field in line.split('\t'):
        cell = subprocess.check_output(
            'eval echo -n "%s"' % field, env=newenv, shell=True)
        curr.append(cell)

      f.write('\t'.join(curr) + '\n')


def get_field_from_tsv(tsv_file, match_column, match_value, return_column):
  """Reads a TSV, matches on a column value and returns another column."""

  with open(tsv_file, 'r') as f:
    input_lines = f.readlines()

  fields = input_lines[0].strip().split('\t')
  match_column_idx = fields.index(match_column)
  return_column_idx = fields.index(return_column)

  for line in input_lines[1:]:
    fields = line.strip().split('\t')
    if re.search(match_value, fields[match_column_idx]):
      return fields[return_column_idx]
