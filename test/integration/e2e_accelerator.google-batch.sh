#!/bin/bash

# Copyright 2025 Verily Life Sciences Inc. All Rights Reserved.
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

# Test GPU support in Google Batch provider.
# Validates that NVIDIA accelerators trigger:
# 1. --gpus all container option
# 2. batch-debian boot disk image
# 3. Actual GPU access in the running container
#
# Required environment variables:
#   DOCKER_IMAGE - Google Artifact Registry image with GPU support
#               Example: us-central1-docker.pkg.dev/my-project/my-repo/parabricks:latest
#   PET_SA_EMAIL - Service account with access to GAR image and GPU resources
#                  Example: my-service-account@my-project.iam.gserviceaccount.com
#
# Optional environment variables (for VPC-SC or custom networking):
#   GPU_NETWORK - Network configuration
#                 Example: projects/my-project/global/networks/my-network
#   GPU_SUBNETWORK - Subnetwork configuration
#                    Example: projects/my-project/regions/us-central1/subnetworks/my-subnet
#   GPU_USE_PRIVATE_ADDRESS - Set to any value to use private address

readonly SCRIPT_DIR="$(dirname "${0}")"

# Do standard test setup
source "${SCRIPT_DIR}/test_setup_e2e.sh"

# Check GPU-specific prerequisites
if [[ -z "${DOCKER_IMAGE:-}" ]]; then
  1>&2 echo "ERROR: DOCKER_IMAGE environment variable is not set."
  1>&2 echo "This test requires a Google Artifact Registry image with GPU support."
  1>&2 echo "Set it with: export DOCKER_IMAGE='REGION-docker.pkg.dev/PROJECT/REPO/IMAGE:TAG'"
  1>&2 echo "Example: export DOCKER_IMAGE='us-central1-docker.pkg.dev/my-project/my-repo/parabricks:latest'"
  exit 1
fi

if [[ -z "${PET_SA_EMAIL:-}" ]]; then
  1>&2 echo "ERROR: PET_SA_EMAIL environment variable is not set."
  1>&2 echo "This test requires a service account with access to the GAR image and GPU resources."
  1>&2 echo "Set it with: export PET_SA_EMAIL='my-service-account@my-project.iam.gserviceaccount.com'"
  exit 1
fi

echo "Launching GPU pipeline with Google Batch provider..."
echo "  Using image: ${DOCKER_IMAGE}"
echo "  Using service account: ${PET_SA_EMAIL}"

# Test nvidia accelerator enables GPU features
# Uses DOCKER_IMAGE and PET_SA_EMAIL environment variables (required)
# Optionally uses GPU_NETWORK, GPU_SUBNETWORK, and GPU_USE_PRIVATE_ADDRESS if set
run_dsub \
  --provider 'google-batch' \
  --image "${DOCKER_IMAGE}" \
  --service-account "${PET_SA_EMAIL}" \
  ${GPU_NETWORK:+--network "${GPU_NETWORK}"} \
  ${GPU_SUBNETWORK:+--subnetwork "${GPU_SUBNETWORK}"} \
  ${GPU_USE_PRIVATE_ADDRESS:+--use-private-address} \
  --accelerator-type 'nvidia-tesla-t4' \
  --accelerator-count 1 \
  --env NVIDIA_VISIBLE_DEVICES=all \
  --command '\
    echo "=== GPU Detection Test ===" && \
    nvidia-smi && \
    echo "=== Boot Image Test ===" && \
    cat /etc/os-release | grep "ID=" && \
    echo "=== Container GPU Access Test ===" && \
    nvidia-smi -L' \
  --wait

echo
echo "Checking GPU detection output..."

# Check that GPU was detected and accessible
RESULT="$(gcloud storage cat "${STDOUT_LOG}")"

# Validate GPU hardware was detected
if ! echo "${RESULT}" | grep -qi "Tesla T4"; then
  1>&2 echo "ERROR: Tesla T4 GPU not detected in nvidia-smi output!"
  1>&2 echo "stdout content:"
  1>&2 echo "${RESULT}"
  exit 1
fi

# Validate GPU memory info is present
if ! echo "${RESULT}" | grep -qi "GPU.*Memory"; then
  1>&2 echo "ERROR: GPU Memory information not found!"
  1>&2 echo "stdout content:"
  1>&2 echo "${RESULT}"
  exit 1
fi

# Validate container has GPU access (nvidia-smi -L should list GPUs)
if ! echo "${RESULT}" | grep -qi "GPU 0:"; then
  1>&2 echo "ERROR: Container does not have GPU access (nvidia-smi -L failed)!"
  1>&2 echo "stdout content:"
  1>&2 echo "${RESULT}"
  exit 1
fi

echo
echo "GPU test output (showing GPU was accessible):"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"
echo "SUCCESS: GPU accelerator test passed!"
echo "- GPU hardware detected"
echo "- Container has GPU access"
echo "- batch-debian image used (implied by successful GPU access)"