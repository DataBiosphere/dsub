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

"""Utililty for helping shell scripts extract values from JSON.

Usage:

  python get_json_value.py JSON-STRING FIELD

The supported syntax can handle simple keys, indexes into arrays, as well
as field matching within arrays.

For example:

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

import json
import re
import sys


def unquote(value):
  """Remove surrounding single or double quotes."""
  if value.startswith('"') and value.endswith('"'):
    return value[1:-1].replace('\\"', '')
  return value


def main(json_string, field):

  if len(sys.argv) != 3:
    print >> sys.stderr, 'Usage: %s JSON FIELD' % sys.argv[0]
    sys.exit(1)

  data = json.loads(json_string)

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
          print >> sys.stderr, 'Cannot value match on non-list object.'
          print >> sys.stderr, 'Key: %s' % key
          print >> sys.stderr, 'Value: %s' % curr

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
        print >> sys.stderr, 'Cannot index into a on non-list object.'
        print >> sys.stderr, 'Key: %s' % key
        print >> sys.stderr, 'Value: %s' % curr

      if idx >= len(curr):
        sys.exit(1)

      curr = curr[idx]
      continue

    # Check for basic key indexing
    if key in curr:
      curr = curr[key]
    else:
      sys.exit(1)

  print curr


if __name__ == '__main__':
  main(sys.argv[1], sys.argv[2])
