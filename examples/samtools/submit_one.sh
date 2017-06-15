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

readonly OUTPUT_ROOT="${MY_BUCKET_PATH}/samtools/submit_one"

readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/pilot_data/data/NA12878/alignment/NA12878.chrom11.SOLID.corona.SRP000032.2009_08.bam"
readonly OUTPUT_BAI="${OUTPUT_ROOT}/output/*.bai"

readonly SCRIPT_DIR="$(dirname "${0}")"

# Launch the task
dsub \
  --project "${MY_PROJECT}" \
  --zones "us-central1-*" \
  --logging "${OUTPUT_ROOT}"/logging \
  --disk-size 200 \
  --name "samtools index" \
  --image quay.io/cancercollaboratory/dockstore-tool-samtools-index \
  --input INPUT_BAM=${INPUT_BAM} \
  --output OUTPUT_BAI="${OUTPUT_BAI}" \
  --command 'export BAI_NAME="$(basename "${INPUT_BAM}").bai"
             samtools index \
               "${INPUT_BAM}" \
               "$(dirname "${OUTPUT_BAI}")/${BAI_NAME}"' \
  --wait
