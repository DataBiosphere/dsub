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

"""Unit tests for GPU support in the Google Batch provider."""

import unittest
from dsub.providers import google_batch
from dsub.lib import job_model


class TestGPUSupport(unittest.TestCase):
    """Test GPU-specific configurations in Google Batch provider."""

    def _create_test_job_descriptor(self, accelerator_type=None, boot_disk_image=None, install_gpu_drivers=None):
        """Create a minimal JobDescriptor for testing.

        Args:
            accelerator_type: The accelerator type to use, or None for no accelerator.
            boot_disk_image: Custom boot disk image, or None for default.
            install_gpu_drivers: Whether to install GPU drivers, or None for default.

        Returns:
            A JobDescriptor configured for testing.
        """
        job_metadata = {
            'script': job_model.Script('test.sh', 'echo hello'),
            'job-id': 'test-job-id',
            'job-name': 'test-job-name',
            'user-id': 'test-user',
            'user-project': 'test-project',
            'dsub-version': '1-0-0'
        }

        job_params = {}
        job_model.ensure_job_params_are_complete(job_params)

        task_metadata = {}
        task_params = {
            'labels': set(),
            'envs': set(),
            'inputs': set(),
            'outputs': set(),
            'input-recursives': set(),
            'output-recursives': set()
        }

        task_resources = job_model.Resources(
            logging_path=job_model.LoggingParam(
                'gs://test-bucket/logs.log', 'google-cloud-storage'
            )
        )
        task_descriptor = job_model.TaskDescriptor(
            task_metadata, task_params, task_resources
        )

        job_resources = job_model.Resources(
            accelerator_type=accelerator_type,
            image='gcr.io/test/image:latest',
            boot_disk_image=boot_disk_image,
            install_gpu_drivers=install_gpu_drivers
        )

        return job_model.JobDescriptor(
            job_metadata, job_params, job_resources, [task_descriptor]
        )

    def _create_batch_request(self, job_descriptor):
        """Create a batch request using the Google Batch provider.

        Args:
            job_descriptor: The JobDescriptor to create a request for.

        Returns:
            The CreateJobRequest object from the provider.
        """
        provider = google_batch.GoogleBatchJobProvider(
            dry_run=True,
            project='test-project',
            location='us-central1'
        )
        return provider._create_batch_request(job_descriptor)

    def test_nvidia_accelerator_enables_gpu_options(self):
        """Test that nvidia accelerators enable GPU-specific configurations."""
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-a100'
        )
        request = self._create_batch_request(job_descriptor)

        # Extract the user command runnable (index 3 in the runnables list)
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]

        # Verify GPU container options are set
        self.assertEqual(user_runnable.container.options, "--gpus all")

        # Verify boot disk uses GPU-compatible image
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, "batch-debian")

    def test_non_nvidia_accelerator_uses_default_options(self):
        """Test that non-nvidia accelerators use default configurations."""
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='tpu-v3'
        )
        request = self._create_batch_request(job_descriptor)

        # Extract the user command runnable
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]

        # Verify no GPU options are set
        self.assertIn(user_runnable.container.options, [None, ""])

        # Verify default boot disk image is used
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, "")

    def test_no_accelerator_uses_default_options(self):
        """Test that jobs without accelerators use default configurations."""
        job_descriptor = self._create_test_job_descriptor(accelerator_type=None)
        request = self._create_batch_request(job_descriptor)

        # Extract the user command runnable
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]

        # Verify no GPU options are set
        self.assertIn(user_runnable.container.options, [None, ""])

        # Verify default boot disk image is used
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, "")

    def test_custom_boot_disk_image_overrides_default(self):
        """Test that custom boot_disk_image overrides the default."""
        custom_image = "projects/deeplearning-platform-release/global/images/family/common-gpu"
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-a100',
            boot_disk_image=custom_image
        )
        request = self._create_batch_request(job_descriptor)

        # Verify custom boot disk image is used instead of batch-debian
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, custom_image)

        # Verify GPU container options are still set
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]
        self.assertEqual(user_runnable.container.options, "--gpus all")

    def test_install_gpu_drivers_false_disables_driver_installation(self):
        """Test that install_gpu_drivers=False disables driver installation."""
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-a100',
            install_gpu_drivers=False
        )
        request = self._create_batch_request(job_descriptor)

        # Verify GPU drivers are not installed
        ipt = request.job.allocation_policy.instances[0]
        self.assertFalse(ipt.install_gpu_drivers)

        # Verify GPU container options are still set
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]
        self.assertEqual(user_runnable.container.options, "--gpus all")

    def test_install_gpu_drivers_true_enables_driver_installation(self):
        """Test that install_gpu_drivers=True enables driver installation."""
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-a100',
            install_gpu_drivers=True
        )
        request = self._create_batch_request(job_descriptor)

        # Verify GPU drivers are installed
        ipt = request.job.allocation_policy.instances[0]
        self.assertTrue(ipt.install_gpu_drivers)

    def test_vpc_sc_scenario_custom_image_no_drivers(self):
        """Test VPC-SC scenario with custom image and no driver installation."""
        custom_image = "projects/deeplearning-platform-release/global/images/family/common-gpu"
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-a100',
            boot_disk_image=custom_image,
            install_gpu_drivers=False
        )
        request = self._create_batch_request(job_descriptor)

        # Verify custom boot disk image is used
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, custom_image)

        # Verify GPU drivers are not installed
        ipt = request.job.allocation_policy.instances[0]
        self.assertFalse(ipt.install_gpu_drivers)

        # Verify GPU container options are still set (containers need GPU access)
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]
        self.assertEqual(user_runnable.container.options, "--gpus all")

    def test_default_install_gpu_drivers_true_for_nvidia(self):
        """Test that install_gpu_drivers defaults to True for NVIDIA accelerators."""
        job_descriptor = self._create_test_job_descriptor(
            accelerator_type='nvidia-tesla-t4'
        )
        request = self._create_batch_request(job_descriptor)

        # Verify GPU drivers are installed by default
        ipt = request.job.allocation_policy.instances[0]
        self.assertTrue(ipt.install_gpu_drivers)

    def test_custom_boot_disk_image_without_accelerator(self):
        """Test that custom boot_disk_image can be used without accelerators."""
        custom_image = "projects/my-project/global/images/my-custom-image"
        job_descriptor = self._create_test_job_descriptor(
            boot_disk_image=custom_image
        )
        request = self._create_batch_request(job_descriptor)

        # Verify custom boot disk image is used
        instance_policy = request.job.allocation_policy.instances[0].policy
        self.assertEqual(instance_policy.boot_disk.image, custom_image)

        # Verify no GPU options are set
        user_runnable = request.job.task_groups[0].task_spec.runnables[3]
        self.assertIn(user_runnable.container.options, [None, ""])

        # Verify GPU drivers are not installed
        ipt = request.job.allocation_policy.instances[0]
        self.assertFalse(ipt.install_gpu_drivers)


if __name__ == '__main__':
    unittest.main()