#!/bin/bash

# Copyright 2021 Verily Life Sciences Inc. All Rights Reserved.
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

# Intended to be used by e2e_block_external_network.sh

set -o errexit
set -o nounset

RC=0

if ! gsutil -o 'Boto:num_retries=0' ls gs://genomics-public-data; then
  1>&2 echo "\`gsutil ls\` should not have succeeded"
  RC=1
fi

if ! curl google.com; then
  1>&2 echo "\`curl google.com\` should not have succeeded"
  RC=1
fi

exit "${RC}"
