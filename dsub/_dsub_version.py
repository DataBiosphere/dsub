# Copyright 2017 Google Inc. All Rights Reserved.
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

"""Single source of truth for dsub's version.

This must remain small and dependency-free so that any dsub module may
import it without creating circular dependencies. Note that this module
is parsed as a text file by setup.py and changes to the format of this
file could break setup.py.

The version should follow formatting requirements specified in PEP-440.
 - https://www.python.org/dev/peps/pep-0440

A typical release sequence will be versioned as:
  0.1.3.dev0 -> 0.1.3 -> 0.1.4.dev0 -> ...
"""

DSUB_VERSION = '0.4.4'
