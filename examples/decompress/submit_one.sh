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
# Wrapper script to decompress a single VCF file and copy it to a Cloud Storage
# bucket.

set -o errexit
set -o nounset

readonly MY_PROJECT=${1}
readonly MY_BUCKET_PATH=${2}

readonly INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz"
readonly OUTPUT_VCF="${MY_BUCKET_PATH}/output/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf"

readonly SCRIPT_DIR="$(dirname "${0}")"

# Launch the task
dsub \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${MY_BUCKET_PATH}"/logging/ \
  --disk-size 200 \
  --image ubuntu:14.04 \
  --input INPUT_VCF=${INPUT_VCF} \
  --output OUTPUT_VCF="${OUTPUT_VCF}" \
  --command 'gunzip ${INPUT_VCF} && \
             mv ${INPUT_VCF%.gz} $(dirname ${OUTPUT_VCF})' \
  --wait
