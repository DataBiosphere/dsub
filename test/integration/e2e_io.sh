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

# Test copying input files to and output files from directories.
#
# This test is designed to verify that named file input and output path
# command-line parameters work correctly.
#
# The actual operation performed here is to download a BAM and compute
# the md5, writing it to <filename>.bam.md5.
#
# An input file (the BAM) is localized to a subdirectory of the default
# data directory.
# An output file (the MD5) is de-localized from a different subdirectory
# of the default data directory.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Launching pipeline..."

  run_dsub \
    --script "${SCRIPT_DIR}/script_io_test.sh" \
    --input INPUT_PATH="${INPUT_BAM}" \
    --output OUTPUT_PATH="${OUTPUTS}/*.md5" \
    --wait

fi

source "${SCRIPT_DIR}/io_check_output.sh"
