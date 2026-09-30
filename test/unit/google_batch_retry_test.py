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
"""Unit tests for retrying Batch list_jobs calls in lookup_job_tasks."""

import unittest
from unittest import mock

from dsub.providers import google_batch
from google.api_core import exceptions as core_exceptions
from google.api_core import gapic_v1
from google.api_core import retry as retries
from google.auth import credentials as auth_credentials
from google.cloud import batch_v1


def _make_provider():
  # Skip __init__, which builds a storage service requiring credentials.
  provider = google_batch.GoogleBatchJobProvider.__new__(
      google_batch.GoogleBatchJobProvider
  )
  provider._project = 'test-project'
  provider._location = 'us-central1'
  return provider


class LookupJobTasksRetryTest(unittest.TestCase):

  def setUp(self):
    # Use a real client with a mocked transport so the real GAPIC retry
    # wrapping is exercised.
    self.client = batch_v1.BatchServiceClient(
        credentials=mock.create_autospec(
            auth_credentials.AnonymousCredentials, instance=True
        )
    )
    patcher = mock.patch.object(
        batch_v1, 'BatchServiceClient', return_value=self.client
    )
    patcher.start()
    self.addCleanup(patcher.stop)
    # Don't actually sleep between retries.
    sleep = mock.patch('time.sleep')
    sleep.start()
    self.addCleanup(sleep.stop)

  def _install_rpc(self, stub):
    # Wrap the stub the same way the client wraps the real gRPC method, with a
    # short default retry that callers are expected to override.
    transport = self.client._transport
    transport._wrapped_methods[transport.list_jobs] = (
        gapic_v1.method.wrap_method(
            stub,
            default_retry=retries.Retry(initial=0.01, timeout=0.05),
            default_timeout=None,
            client_info=gapic_v1.client_info.ClientInfo(),
        )
    )

  def test_retries_deadline_exceeded(self):
    stub = mock.Mock(
        side_effect=[
            core_exceptions.DeadlineExceeded('504 Deadline Exceeded'),
            core_exceptions.DeadlineExceeded('504 Deadline Exceeded'),
            batch_v1.ListJobsResponse(jobs=[]),
        ]
    )
    self._install_rpc(stub)
    tasks = list(_make_provider().lookup_job_tasks({'*'}))

    self.assertEqual([], tasks)
    self.assertEqual(3, stub.call_count)

  def test_does_not_retry_permanent_errors(self):
    stub = mock.Mock(side_effect=core_exceptions.PermissionDenied('denied'))
    self._install_rpc(stub)
    with self.assertRaises(core_exceptions.PermissionDenied):
      list(_make_provider().lookup_job_tasks({'*'}))

    self.assertEqual(1, stub.call_count)


if __name__ == '__main__':
  unittest.main()
