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

echo
echo "Checking output..."

declare -a INPUT_BAMS=(
NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
)

declare -a RESULTS_EXPECTED=(
4afb9b8908959dbd4e2d5c54bf254c93
0dc006ed39ddad2790034ca497631234
36e37a0dab5926dbf5a1b8afc0cdac8b
)

for ((i=0; i < ${#INPUT_BAMS[@]}; i++)); do
  INPUT_BAM="${INPUT_BAMS[i]}"
  RESULT_EXPECTED="${RESULTS_EXPECTED[i]}"

  OUTPUT_PATH="$(grep "${INPUT_BAM}" "${TASKS_FILE}" | cut -d $'\t' -f 3)"
  OUTPUT_FILE="${OUTPUT_PATH%/*.md5}/$(basename "${INPUT_BAM}").md5"
  RESULT="$(gsutil cat "${OUTPUT_FILE}")"

  if ! diff <(echo "${RESULT_EXPECTED}") <(echo "${RESULT}"); then
    echo "Output file does not match expected"
    exit 1
  fi

  echo
  echo "Output file matches expected:"
  echo "*****************************"
  echo "${RESULT}"
  echo "*****************************"
done

echo "SUCCESS"
