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

declare -a INPUT_BAMS=(
NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
)

declare -a INPUT_BAMS_MD5=(
4afb9b8908959dbd4e2d5c54bf254c93
0dc006ed39ddad2790034ca497631234
36e37a0dab5926dbf5a1b8afc0cdac8b
)

function io_tasks_setup::run_dsub() {
  local io_tasks_script="${1}"
  local io_tasks_file="${2}"

  run_dsub \
    --script "${io_tasks_script}" \
    --tasks "${io_tasks_file}" \
    --env TEST_NAME="${TEST_NAME}" \
    --input POPULATION_FILE="${POPULATION_FILE}" \
    --output OUTPUT_POPULATION_FILE="${OUTPUTS}/*" \
    --wait
}
readonly -f io_tasks_setup::run_dsub

function io_tasks_setup::check_output() {
  echo
  echo "Checking output..."

  readonly tasks_count="${#INPUT_BAMS[@]}"

  # Check the MD5s for each of the BAMs
  for ((i=0; i < tasks_count; i++)); do
    input_bam="${INPUT_BAMS[i]}"
    expected="${INPUT_BAMS_MD5[i]}"

    output_path="$(grep "${input_bam}" "${TASKS_FILE}" | cut -d $'\t' -f 3)"
    output_file="${output_path%/*.md5}/$(basename "${input_bam}").md5"
    result="$(gsutil cat "${output_file}")"

    if ! diff <(echo "${expected}") <(echo "${result}"); then
      echo "Output file does not match expected"
      exit 1
    fi

    echo
    echo "Output file matches expected:"
    echo "*****************************"
    echo "${result}"
    echo "*****************************"
  done

  # Check that the population file got copied for each of the tasks
  expected="${POPULATION_MD5}"
  for ((i=0; i < tasks_count; i++)); do
    output_file="${OUTPUTS}/TASK_$((i+1)).md5"
    result="$(gsutil cat "${output_file}")"

    if ! diff <(echo "${expected}") <(echo "${result}"); then
      echo "Output file does not match expected"
      exit 1
    fi

    echo
    echo "Output file matches expected:"
    echo "*****************************"
    echo "${result}"
    echo "*****************************"
  done

  echo "SUCCESS"
}
readonly -f io_tasks_setup::check_output
