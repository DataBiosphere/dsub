#!/bin/bash

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

# submit_list.sh
#
# Usage:
#   submit_list.sh PROJECT-ID BUCKET
#
# Wrapper script to decompress a list of VCF files and copy each to a
# Cloud Storage bucket. Each VCF is decompressed as a separate task.
#
# Edit the submit_list.tsv file to replace MY-BUCKET with your bucket.

set -o errexit
set -o nounset

readonly MY_PROJECT=${1}
readonly MY_BUCKET=${2}

readonly SCRIPT_DIR="$(dirname "${0}")"

# Launch the task
dsub \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${MY_BUCKET}/decompress_list/logging/" \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --tasks "${SCRIPT_DIR}"/submit_list.tsv \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --wait
