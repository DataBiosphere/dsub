#!/usr/bin/python

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

"""Utililty for helping shell scripts extract values from JSON or YAML.

Usage:

  python get_data_value.py DOC-TYPE DOC-STRING FIELD

The supported syntax can handle simple keys, indexes into arrays, as well
as field matching within arrays.

For example (JSON):

{
  "data": {
    "more_data": [{
      "name": "ARG1",
      "array_data": [{
        "field": "arg1_value1"
      },{
        "field": "arg1_value2"
      },{
        "field": "arg1_value3"
      }]
    },{
      "name": "ARG2",
      "array_data": [{
        "field": "arg2_value1"
      },{
        "field": "arg2_value2"
      },{
        "field": "arg2_value3"
      }]
    }]
  }
}

To extract "arg1_value2", use the path:

  data.more_data.{name="ARG1"}.array_data.[1].field

"""
from __future__ import print_function

import json
import re
import sys
import yaml


def unquote(value):
  """Remove surrounding single or double quotes."""
  if value.startswith('"') and value.endswith('"'):
    return value[1:-1].replace('\\"', '')
  return value


def main():

  if len(sys.argv) != 4:
    print('Usage: %s [json|yaml] DOC-STRING FIELD' % sys.argv[0],
          file=sys.stderr)
    sys.exit(1)

  doc_type = sys.argv[1]
  doc_string = sys.argv[2]
  field = sys.argv[3]

  if doc_type == 'json':
    data = json.loads(doc_string)
  elif doc_type == 'yaml':
    try:
      data = yaml.full_load(doc_string)
    except AttributeError:
      # For installations that cannot update their PyYAML version
      data = yaml.load(doc_string)
  else:
    raise ValueError('Unsupported doc type: %s' % doc_type)

  # field is expected to be period-separated: foo.bar.baz
  fields = field.split('.')

  # Walk the list of fields and check that the key exists.
  curr = data
  for key in fields:
    # Check for matching on key=value pairs. For example:
    #   {name='NAME1'}
    if key.startswith('{') and key.endswith('}'):
      m = re.match(r'{(.*)=(.*)}', key)
      if m:
        name = m.group(1)
        value = unquote(m.group(2))

        if not isinstance(curr, list):
          print('Cannot value match on non-list object.', file=sys.stderr)
          print('Key: %s' % key, file=sys.stderr)
          print('Value: %s' % curr, file=sys.stderr)
          sys.exit(1)

        found = None
        for item in curr:
          if item[name] == value:
            found = item
            break

        if found:
          curr = found
          continue

    # Check for array indexing
    if key.startswith('[') and key.endswith(']'):
      idx = int(key[1:-1])
      if not isinstance(curr, list):
        print('Cannot index into a non-list object.', file=sys.stderr)
        print('Key: %s' % key, file=sys.stderr)
        print('Value: %s' % curr, file=sys.stderr)
        sys.exit(1)

      if idx >= len(curr):
        print('Index of key out of bounds', file=sys.stderr)
        print('Key: %s' % key, file=sys.stderr)
        print('Value: %s' % curr, file=sys.stderr)
        sys.exit(1)

      curr = curr[idx]
      continue

    # Check for basic key indexing
    if key in curr:
      curr = curr[key]
    else:
      print('Key not found', file=sys.stderr)
      print('Key: %s' % key, file=sys.stderr)
      print('Value: %s' % curr, file=sys.stderr)
      sys.exit(1)

  print(curr)


if __name__ == '__main__':
  main()
