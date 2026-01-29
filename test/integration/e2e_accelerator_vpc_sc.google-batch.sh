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

# Test GPU support in VPC-SC environments with Google Batch provider.
# Validates that custom boot disk images and driver installation flags work correctly:
# 1. Custom boot disk image with pre-installed drivers
# 2. --install-gpu-drivers false to skip driver downloads
# 3. VPC network configuration for VPC-SC perimeters
# 4. Private IP only mode for VPC-SC compliance
# 5. Actual GPU access in the running container with pre-installed drivers
#
# REQUIRED ENVIRONMENT VARIABLES:
#   DOCKER_IMAGE       - GPU-enabled container image from Google Artifact Registry
#                     Example: us-central1-docker.pkg.dev/my-project/my-repo/image:tag
#   PET_SA_EMAIL    - Service account email with access to GAR image and GPU resources
#                     Example: my-sa@my-project.iam.gserviceaccount.com
#   YOUR_BUCKET     - GCS bucket name for test outputs (do NOT include gs:// prefix)
#                     Example: my-test-bucket
#   GPU_NETWORK     - VPC network for VPC-SC perimeters
#                     Example: projects/my-project/global/networks/network
#   GPU_SUBNETWORK  - VPC subnetwork for VPC-SC perimeters (must match REGIONS)
#                     Example: projects/my-project/regions/us-west2/subnetworks/subnetwork
#   REGIONS         - GCP region where the job will run (must match subnetwork region)
#                     Example: us-west2
#
# OPTIONAL ENVIRONMENT VARIABLES:
#   BOOT_DISK_IMAGE - Custom boot disk image with pre-installed GPU drivers
#                     Default: projects/${PROJECT_ID}/global/images/deeplearning-driver
#                     Alternative: projects/deeplearning-platform-release/global/images/family/common-cu121-debian-11-py310
#
# USAGE:
#   export DOCKER_IMAGE='us-central1-docker.pkg.dev/my-project/my-repo/image:tag'
#   export PET_SA_EMAIL='my-sa@my-project.iam.gserviceaccount.com'
#   export YOUR_BUCKET='my-test-bucket'  # Do NOT include gs://
#   export GPU_NETWORK='projects/my-project/global/networks/network'
#   export GPU_SUBNETWORK='projects/my-project/regions/us-west2/subnetworks/subnetwork'
#   export REGIONS='us-west2'  # Must match subnetwork region
#   ./test/integration/e2e_accelerator_vpc_sc.google-batch.sh

set -o errexit
set -o nounset

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

if [[ -z "${YOUR_BUCKET:-}" ]]; then
  1>&2 echo "ERROR: YOUR_BUCKET environment variable is not set."
  1>&2 echo "This test requires a GCS bucket for test outputs."
  1>&2 echo "Set it with: export YOUR_BUCKET='my-test-bucket' (do NOT include gs:// prefix)"
  exit 1
fi

if [[ -z "${GPU_NETWORK:-}" ]]; then
  1>&2 echo "ERROR: GPU_NETWORK environment variable is not set."
  1>&2 echo "This VPC-SC test requires a VPC network configuration."
  1>&2 echo "Set it with: export GPU_NETWORK='projects/\${GOOGLE_CLOUD_PROJECT}/global/networks/network'"
  exit 1
fi

if [[ -z "${GPU_SUBNETWORK:-}" ]]; then
  1>&2 echo "ERROR: GPU_SUBNETWORK environment variable is not set."
  1>&2 echo "This VPC-SC test requires a VPC subnetwork configuration."
  1>&2 echo "Set it with: export GPU_SUBNETWORK='projects/\${GOOGLE_CLOUD_PROJECT}/regions/us-west2/subnetworks/subnetwork'"
  exit 1
fi

if [[ -z "${REGIONS:-}" ]]; then
  1>&2 echo "ERROR: REGIONS environment variable is not set."
  1>&2 echo "This VPC-SC test requires specifying the region (must match subnetwork region)."
  1>&2 echo "Set it with: export REGIONS='us-west2'"
  exit 1
fi

# Optional: Custom boot disk image (defaults to project-specific deeplearning-driver image)
# For VPC-SC environments, this should be an image with pre-installed GPU drivers
if [[ -z "${BOOT_DISK_IMAGE:-}" ]]; then
  # Default to custom Deep Learning image in the project
  # This assumes you have created a custom image with GPU drivers pre-installed
  BOOT_DISK_IMAGE="projects/${PROJECT_ID}/global/images/deeplearning-driver"
  echo "Using default boot disk image: ${BOOT_DISK_IMAGE}"
else
  echo "Using custom boot disk image: ${BOOT_DISK_IMAGE}"
fi

echo "Launching GPU pipeline in VPC-SC mode with Google Batch provider..."
echo "  Using GAR image: ${DOCKER_IMAGE}"
echo "  Using service account: ${PET_SA_EMAIL}"
echo "  Using boot disk image: ${BOOT_DISK_IMAGE}"
echo "  Using VPC network: ${GPU_NETWORK}"
echo "  Using VPC subnetwork: ${GPU_SUBNETWORK}"
echo "  Region: ${REGIONS}"
echo "  Install GPU drivers: false"
echo "  Private IP only: true"

# Test VPC-SC scenario with custom boot image and no driver installation
# Uses required VPC-SC parameters: GPU_NETWORK, GPU_SUBNETWORK, REGIONS
# Note: Calls dsub directly (not run_dsub) to avoid hardcoded network defaults in test_setup.sh
dsub \
  --provider 'google-batch' \
  --project "${PROJECT_ID}" \
  --regions "${REGIONS}" \
  --logging "${LOGGING}" \
  --image "${DOCKER_IMAGE}" \
  --service-account "${PET_SA_EMAIL}" \
  --network "${GPU_NETWORK}" \
  --subnetwork "${GPU_SUBNETWORK}" \
  --use-private-address \
  --boot-disk-image "${BOOT_DISK_IMAGE}" \
  --boot-disk-size 200 \
  --install-gpu-drivers false \
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
RESULT="$(gsutil cat "${STDOUT_LOG}")"

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
echo "VPC-SC GPU test output (showing GPU was accessible with pre-installed drivers):"
echo "*****************************"
echo "${RESULT}"
echo "*****************************"
echo "SUCCESS: VPC-SC GPU accelerator test passed!"
echo "- GPU hardware detected"
echo "- Container has GPU access"
echo "- Custom boot disk image used: ${BOOT_DISK_IMAGE}"
echo "- GPU drivers pre-installed (driver installation was disabled)"
