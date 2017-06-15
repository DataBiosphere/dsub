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
#   submit_one.sh PROJECT-ID BUCKET SCRIPT
#
# Wrapper script to split a single VCF file into per-chromosome VCF
# files and copy them to a Cloud Storage bucket.

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

readonly OUTPUT_ROOT="gs://${MY_BUCKET_PATH}/$(basename "${SCRIPT}")"

readonly INPUT_VCF="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/working/20130723_phase3_wg/cornell/ALL.ChrY.Cornell.20130502.SNPs.Genotypes.vcf.gz"
readonly OUTPUT_FILE="${OUTPUT_ROOT}/output/sample_ids.txt"
readonly LOGGING="${OUTPUT_ROOT}/logging"

echo "Output will be written to:"
echo "  ${OUTPUT_FILE}"
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
  --input INPUT_VCF="${INPUT_VCF}" \
  --output OUTPUT_FILE="${OUTPUT_FILE}" \
  --wait

# Check output
echo "Check the head of the output file:"
2>&1 gsutil cat "${OUTPUT_FILE}" | head

