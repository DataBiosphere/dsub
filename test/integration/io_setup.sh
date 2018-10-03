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

readonly GENOMICS_PUBLIC_BUCKET="gs://genomics-public-data"
readonly POPULATION_FILE="ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.superpopulations.tsv"
readonly POPULATION_FILE_FULL_PATH="${GENOMICS_PUBLIC_BUCKET}"/"${POPULATION_FILE}"
readonly POPULATION_MD5="68a73f849b82071afe11888bac1aa8a7"

readonly INPUT_BAM_FILE="ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"
readonly INPUT_BAM_FULL_PATH="${GENOMICS_PUBLIC_BUCKET}/${INPUT_BAM_FILE}"
readonly INPUT_BAM_MD5="4afb9b8908959dbd4e2d5c54bf254c93"

function io_setup::run_dsub_fuse() {
  run_dsub \
    ${IMAGE:+--image "${IMAGE}"} \
    --script "${SCRIPT_DIR}/script_io_test.sh" \
    --env TASK_ID="task" \
    --output OUTPUT_PATH="${OUTPUTS}/task/*.md5" \
    --env TEST_NAME="${TEST_NAME}" \
    --env INPUT_BAM="${INPUT_BAM_FILE}" \
    --env POPULATION_FILE="${POPULATION_FILE}" \
    --mount GENOMICS_PUBLIC_BUCKET="${GENOMICS_PUBLIC_BUCKET}" \
    --output OUTPUT_POPULATION_FILE="${OUTPUTS}/*" \
    --wait
}
readonly -f io_setup::run_dsub_fuse

function io_setup::run_dsub() {
  run_dsub \
    ${IMAGE:+--image "${IMAGE}"} \
    --script "${SCRIPT_DIR}/script_io_test.sh" \
    --env TASK_ID="task" \
    --input INPUT_PATH="${INPUT_BAM_FULL_PATH}" \
    --output OUTPUT_PATH="${OUTPUTS}/task/*.md5" \
    --env TEST_NAME="${TEST_NAME}" \
    --input POPULATION_FILE_PATH="${POPULATION_FILE_FULL_PATH}" \
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
    "${OUTPUTS}/task/$(basename "${INPUT_BAM_FULL_PATH}").md5" \
    "${INPUT_BAM_MD5}"

  io_setup::_check_output \
    "${OUTPUTS}/task.md5" \
    "${POPULATION_MD5}"

  echo "SUCCESS"
}
readonly -f io_setup::check_output

function io_setup::check_dstat() {
  local job_id="${1}"
  local check_inputs="${2}"
  local check_mounts="${3}"

  echo
  echo "Checking dstat output..."

  local dstat_output=$(run_dstat --status '*' --jobs "${job_id}" --full)

  echo "  Checking user-id"
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].user-id" "${USER}"

  echo "  Checking logging"
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].logging" "${LOGGING}"

  echo "  Checking status"
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].status" "SUCCESS"

  echo "  Checking datetime fields..."
  for field in 'create-time' 'end-time' 'start-time' 'last-update'; do
    if ! util::dstat_yaml_job_has_valid_datetime_field "${dstat_output}" "[0].${field}"; then
      echo "dstat output for ${job_id} does not include a valid ${field}."
      echo "${dstat_output}"
      exit 1
    fi
  done

  echo "  Checking envs..."
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].envs.TASK_ID" "task"
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].envs.TEST_NAME" "${TEST_NAME}"

  if [[ "${check_inputs}" == "true" ]]; then
    echo "  Checking inputs..."
    util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].inputs.INPUT_PATH" "${INPUT_BAM_FULL_PATH}"
    util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].inputs.POPULATION_FILE_PATH" "${POPULATION_FILE_FULL_PATH}"
  fi

  echo "  Checking outputs..."
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].outputs.OUTPUT_PATH" "${OUTPUTS}/task/*.md5"
  util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].outputs.OUTPUT_POPULATION_FILE" "${OUTPUTS}/*"

  if [[ "${check_mounts}" == "true" ]]; then
    echo "  Checking mounts..."
    util::dstat_yaml_assert_field_equal "${dstat_output}" "[0].mounts.GENOMICS_PUBLIC_BUCKET" "${GENOMICS_PUBLIC_BUCKET}"
  fi

  echo "SUCCESS"
}
readonly -f io_setup::check_dstat
