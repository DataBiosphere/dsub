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

# submit_one.sh
#
# Usage:
#   submit_one.sh PROJECT-ID BUCKET-PATH
#
# Launcher script to index a BAM file and copy the index to a Cloud Storage
# bucket.

set -o errexit
set -o nounset

readonly MY_PROJECT=${1}
readonly MY_BUCKET_PATH=${2}

readonly CONTAINER_PROJECT="$(echo "${MY_PROJECT}" | sed 's_:_/_')"
readonly OUTPUT_ROOT="${MY_BUCKET_PATH}/fastqc/submit_one"
readonly SCRIPT_DIR="$(dirname "${0}")"

readonly OUTPUT_FILES="${OUTPUT_ROOT}/output/*"
readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom19.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"

# Build the docker image
gcloud container builds submit "${SCRIPT_DIR}" \
  --tag="gcr.io/${CONTAINER_PROJECT}/fastqc"

# Launch the task
dsub \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${OUTPUT_ROOT}/logging" \
  --disk-size 200 \
  --name "fastqc" \
  --image "gcr.io/${CONTAINER_PROJECT}/fastqc" \
  --output OUTPUT_FILES="${OUTPUT_FILES}" \
  --input INPUT_BAM="${INPUT_BAM}" \
  --command 'fastqc ${INPUT_BAM} --outdir=$(dirname ${OUTPUT_FILES})' \
  --wait
