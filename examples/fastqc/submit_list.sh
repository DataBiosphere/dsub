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
# Launcher script to index a list of BAM files and copy the index files
# to a Cloud Storage bucket. Each BAM is indexed as a separate task.
#
# Edit the submit_list.tsv file to replace MY-BUCKET with your bucket.

set -o errexit
set -o nounset

readonly MY_PROJECT=${1}
readonly MY_BUCKET_PATH=${2}

readonly CONTAINER_PROJECT="$(echo "${MY_PROJECT}" | sed 's_:_/_')"
readonly OUTPUT_ROOT="${MY_BUCKET_PATH}/fastqc/submit_list"
readonly SCRIPT_DIR="$(dirname "${0}")"

# Build the docker image
gcloud builds submit "${SCRIPT_DIR}" \
  --tag="gcr.io/${CONTAINER_PROJECT}/fastqc"

# Launch the task
dsub \
  --provider google-cls-v2 \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${OUTPUT_ROOT}/logging/" \
  --disk-size 200 \
  --name "fastqc" \
  --image "gcr.io/${CONTAINER_PROJECT}/fastqc" \
  --tasks "${SCRIPT_DIR}/submit_list.tsv" \
  --command 'fastqc ${INPUT_BAM} --outdir=$(dirname ${OUTPUT_FILES})' \
  --wait
