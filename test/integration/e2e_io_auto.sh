#!/bin/bash

# Copyright 2016 Google Inc. All Rights Reserved.
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

set -o errexit
set -o nounset

# Test copying input files and output files where input and output
# parameter names are completely omitted.
#
# This test use the default stock image (ubuntu:14.04).

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"

readonly INPUT_BAMS="gs://genomics-public-data/test-data/dna/wgs/hiseq2500/NA12878/H06HDADXX130110.*.ATCACGAT.20k_reads.bam"

readonly OUTPUT_FILE="${OUTPUTS}/output_file/file.txt"

readonly OUTPUT_DIR="${OUTPUTS}/output_files/*"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  run_dsub \
    --script "${SCRIPT_DIR}/script_io_auto.sh" \
    --env TEST_NAME="${TEST_NAME}" \
    --input "${INPUT_BAM}" "${INPUT_BAMS}" \
    --output "${OUTPUT_FILE}" "${OUTPUT_DIR}" \
    --wait

fi

echo
echo "Checking output..."

readonly EXPECTED_IO_VARS=(
OUTPUT_0=/mnt/data/output/"${DOCKER_OUTPUTS}"/output_file/file.txt
OUTPUT_1=/mnt/data/output/"${DOCKER_OUTPUTS}"/output_files/*
INPUT_0=/mnt/data/input/gs/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
INPUT_1=/mnt/data/input/gs/genomics-public-data/test-data/dna/wgs/hiseq2500/NA12878/
)

readonly EXPECTED_FS_INPUT_ENTRIES=(
/mnt/data/input/gs/genomics-public-data/test-data/dna/wgs/hiseq2500/NA12878/H06HDADXX130110.1.ATCACGAT.20k_reads.bam
/mnt/data/input/gs/genomics-public-data/test-data/dna/wgs/hiseq2500/NA12878/H06HDADXX130110.2.ATCACGAT.20k_reads.bam
/mnt/data/input/gs/genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
)

readonly EXPECTED_FS_OUTPUT_ENTRIES=(
/mnt/data/output/"${DOCKER_OUTPUTS}"/output_files
/mnt/data/output/"${DOCKER_OUTPUTS}"/output_file
)

# Get the results- "env" and "find" output is bounded by "BEGIN" and "END"
readonly RESULT=$(gsutil cat "${STDOUT_LOG}")
readonly ENV=$(echo "${RESULT}" | sed -n '/^BEGIN: env$/,/^END: env$/p')
readonly FIND=$(echo "${RESULT}" | sed -n '/^BEGIN: find$/,/^END: find$/p')

for REC in "${EXPECTED_IO_VARS[@]}"; do
  if ! echo "${ENV}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Output does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${ENV}"
    exit 1
  fi
done

for REC in "${EXPECTED_FS_INPUT_ENTRIES[@]}" "${EXPECTED_FS_OUTPUT_ENTRIES[@]}"; do
  if ! echo "${FIND}" | grep --quiet --fixed-strings "${REC}"; then
    2>&1 echo "Output does not match expected"
    2>&1 echo "Did not find ${REC} in:"
    2>&1 echo "${FIND}"
    exit 1
  fi
done

echo
echo "Output file matches expected:"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"

echo "SUCCESS"
