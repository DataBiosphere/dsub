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


class _RetryTestBase(unittest.TestCase):
  """Real Batch client over a mocked transport."""

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

  def _install_rpc(self, stub, method='list_jobs'):
    # Wrap the stub the same way the client wraps the real gRPC method, with a
    # short default retry that callers are expected to override.
    transport = self.client._transport
    rpc = getattr(transport, method)
    transport._wrapped_methods[rpc] = (
        gapic_v1.method.wrap_method(
            stub,
            default_retry=retries.Retry(initial=0.01, timeout=0.05),
            default_timeout=None,
            client_info=gapic_v1.client_info.ClientInfo(),
        )
    )



class LookupJobTasksRetryTest(_RetryTestBase):

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

  def test_outer_retry_restarts_fetch_when_later_page_fails(self):
    # Disable the inner retry so the error surfaces to the outer layer.
    no_inner_retry = mock.patch.object(
        google_batch, '_LIST_JOBS_RETRY', retries.Retry(predicate=lambda e: False)
    )
    no_inner_retry.start()
    self.addCleanup(no_inner_retry.stop)
    job = batch_v1.Job(name='projects/p/locations/l/jobs/j')
    stub = mock.Mock(
        side_effect=[
            batch_v1.ListJobsResponse(jobs=[job], next_page_token='page2'),
            core_exceptions.DeadlineExceeded('504 Deadline Exceeded'),
            batch_v1.ListJobsResponse(jobs=[job], next_page_token='page2'),
            batch_v1.ListJobsResponse(jobs=[job]),
        ]
    )
    self._install_rpc(stub)
    with mock.patch.object(
        google_batch,
        'GoogleBatchOperation',
        side_effect=lambda j: mock.Mock(get_field=lambda f: j.name),
    ):
      tasks = list(_make_provider().lookup_job_tasks({'*'}))

    self.assertEqual(2, len(tasks))
    self.assertEqual(4, stub.call_count)

  def test_outer_retry_gives_up_after_max_attempts(self):
    no_inner_retry = mock.patch.object(
        google_batch, '_LIST_JOBS_RETRY', retries.Retry(predicate=lambda e: False)
    )
    no_inner_retry.start()
    self.addCleanup(no_inner_retry.stop)
    stub = mock.Mock(side_effect=core_exceptions.DeadlineExceeded('504'))
    self._install_rpc(stub)
    with self.assertRaises(core_exceptions.DeadlineExceeded):
      list(_make_provider().lookup_job_tasks({'*'}))

    self.assertEqual(google_batch._LIST_JOBS_OUTER_ATTEMPTS, stub.call_count)


class SubmitBatchJobRetryTest(_RetryTestBase):
  """Tests retrying create_job in _submit_batch_job."""

  def _request(self):
    return batch_v1.CreateJobRequest(
        parent='projects/p/locations/l', job_id='j', job=batch_v1.Job()
    )

  def _submit(self):
    provider = _make_provider()
    with mock.patch.object(
        google_batch,
        'GoogleBatchOperation',
        side_effect=lambda j: mock.Mock(get_field=lambda f: j.name),
    ):
      return provider._submit_batch_job(self._request())

  def test_retries_deadline_exceeded_on_create(self):
    job = batch_v1.Job(name='projects/p/locations/l/jobs/j')
    stub = mock.Mock(
        side_effect=[core_exceptions.DeadlineExceeded('504'), job]
    )
    self._install_rpc(stub, 'create_job')

    self.assertEqual(job.name, self._submit())
    self.assertEqual(2, stub.call_count)

  def test_already_exists_after_retry_returns_existing_job(self):
    job = batch_v1.Job(name='projects/p/locations/l/jobs/j')
    create = mock.Mock(
        side_effect=[
            core_exceptions.DeadlineExceeded('504'),
            core_exceptions.AlreadyExists('exists'),
        ]
    )
    get = mock.Mock(return_value=job)
    self._install_rpc(create, 'create_job')
    self._install_rpc(get, 'get_job')

    self.assertEqual(job.name, self._submit())
    get.assert_called_once()

  def test_already_exists_on_first_attempt_raises(self):
    stub = mock.Mock(side_effect=core_exceptions.AlreadyExists('exists'))
    self._install_rpc(stub, 'create_job')
    with self.assertRaises(core_exceptions.AlreadyExists):
      self._submit()

    self.assertEqual(1, stub.call_count)


if __name__ == '__main__':
  unittest.main()
