# Lint as: python3
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
"""Enable dsub methods to access resources in dsub packages.

dsub, dstat, and ddel are designed to run under a few different packaging
environments. This module implements access to resources (such as static
text files) for the setuptools distribution.

This module is imported by dsub.py, dstat.py, and ddel.py and passed down to
other classes that may need it. This module should not otherwise be imported
directly.

This mechanism allows users of dsub.py, dstat.py, and ddel.py to replace the
resources module with their own resource package after important and before
calling main() or other entrypoints.
"""

import os

# The resource root is the root dsub directory.
# For example:
#   my_dir/dsub/dsub/lib/resources.py --> my_dir/dsub
_RESOURCE_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def get_resource(resource_path, mode='rb'):
  with open(os.path.join(_RESOURCE_ROOT, resource_path), mode=mode) as f:
    return f.read()
