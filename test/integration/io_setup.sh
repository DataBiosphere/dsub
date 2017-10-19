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

readonly POPULATION_FILE="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.superpopulations.tsv"
readonly POPULATION_MD5="68a73f849b82071afe11888bac1aa8a7"

readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"
readonly INPUT_BAM_MD5="4afb9b8908959dbd4e2d5c54bf254c93"

function io_setup::run_dsub() {
  run_dsub \
    ${IMAGE:+--image "${IMAGE}"} \
    --script "${SCRIPT_DIR}/script_io_test.sh" \
    --env TASK_ID="task" \
    --input INPUT_PATH="${INPUT_BAM}" \
    --output OUTPUT_PATH="${OUTPUTS}/task/*.md5" \
    --env TEST_NAME="${TEST_NAME}" \
    --input POPULATION_FILE="${POPULATION_FILE}" \
    --output OUTPUT_POPULATION_FILE="${OUTPUTS}/*" \
    --wait
}
readonly -f io_setup::run_dsub

function io_setup::_check_output() {
  local output_file="${1}"
  local result_expected="${2}"

  local result=$(gsutil cat "${output_file}")
  if ! diff <(echo "${result_expected}") <(echo "${result}"); then
    echo "Output file does not match expected"
    exit 1
  fi

  echo
  echo "Output file matches expected:"
  echo "*****************************"
  echo "${result}"
  echo "*****************************"
}
readonly -f io_setup::_check_output

function io_setup::check_output() {
  echo
  echo "Checking output..."

  io_setup::_check_output \
    "${OUTPUTS}/task/$(basename "${INPUT_BAM}").md5" \
    "${INPUT_BAM_MD5}"

  io_setup::_check_output \
    "${OUTPUTS}/task.md5" \
    "${POPULATION_MD5}"

  echo "SUCCESS"
}
readonly -f io_setup::check_output
