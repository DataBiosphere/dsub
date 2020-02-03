# Copyright 2020 Verily Life Sciences Inc. All Rights Reserved.
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
"""Constants and methods for Google's Pipelines API v2."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

V2ALPHA1 = 'v2alpha1'
V2BETA = 'v2beta'


def get_api_name(version):
  if version == V2ALPHA1:
    return 'genomics'

  if version == V2BETA:
    return 'lifesciences'

  assert False, 'Unsupported API version: {}'.format(version)


if __name__ == '__main__':
  pass
