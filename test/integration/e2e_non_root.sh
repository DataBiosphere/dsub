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

set -o errexit
set -o nounset

# Test a Docker image that sets the USER to a non-root user.
# This test is otherwise identical to e2e_io.sh.

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

readonly INPUT_BAM="gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam"

readonly BUILD_DIR="${TEST_TEMP}/${TEST_NAME}"
readonly DOCKERFILE="${BUILD_DIR}/Dockerfile"
readonly IMAGE=gcr.io/${PROJECT_ID}/dsub_usertest:$(date +%Y-%m-%d)_${RANDOM}

function exit_handler() {
  local code="${?}"

  gcloud --quiet container images delete "${IMAGE}"

  return "${code}"
}
readonly -f exit_handler

trap "exit_handler" EXIT

if [[ "${CHECK_RESULTS_ONLY:-0}" -eq 0 ]]; then

  echo "Enabling Google Container Builder"
  gcloud service-management enable cloudbuild.googleapis.com

  echo "Creating image using Google Container Builder"

  mkdir -p "${BUILD_DIR}"
  sed -e 's#^ *##' > "${DOCKERFILE}" <<-EOF
    FROM ubuntu:14.04

    RUN adduser test_user \
      --disabled-password --gecos "First Last,RoomNumber,WorkPhone,HomePhone"

    USER test_user
EOF

  gcloud container builds submit "${BUILD_DIR}" --tag "${IMAGE}"

  echo "Launching pipeline..."

  run_dsub \
    --image "${IMAGE}" \
    --script "${SCRIPT_DIR}/script_io_test.sh" \
    --input INPUT_PATH="${INPUT_BAM}" \
    --output OUTPUT_PATH="${OUTPUTS}/*.md5" \
    --wait

fi

source "${SCRIPT_DIR}/io_check_output.sh"
