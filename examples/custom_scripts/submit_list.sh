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
#   submit_list.sh PROJECT-ID BUCKET SCRIPT
#
# Wrapper script to decompress a list of VCF files and copy each to a
# Cloud Storage bucket. Each VCF is decompressed as a separate task.
#
# Edit the submit_list.tsv file to replace MY-BUCKET with your bucket.

set -o errexit
set -o nounset

readonly SCRIPT_DIR="$(dirname "${0}")"

if [[ $# -ne 3 ]]; then
  2>&1 echo "Usage: ${0} project-id bucket script"
  2>&1 echo
  2>&1 echo "  script is either of:"
  2>&1 echo "    ${SCRIPT_DIR}/get_vcf_sample_ids.sh"
  2>&1 echo "    ${SCRIPT_DIR}/get_vcf_sample_ids.py"
  exit 1
fi

readonly MY_PROJECT=${1}
readonly MY_BUCKET_PATH=${2}
readonly SCRIPT=${3}

declare IMAGE="ubuntu:14.04"
if [[ ${SCRIPT} == *.py ]]; then
  IMAGE="python:2.7"
fi
readonly IMAGE

readonly OUTPUT_ROOT="gs://${MY_BUCKET_PATH}/get_vcf_sample_ids"
readonly LOGGING="${OUTPUT_ROOT}/logging"

echo "Logging will be written to:"
echo "  ${LOGGING}"
echo

# Launch the task
dsub \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${LOGGING}" \
  --disk-size 200 \
  --image "${IMAGE}" \
  --script "${SCRIPT}" \
  --tasks "${SCRIPT_DIR}"/submit_list.tsv \
  --wait
