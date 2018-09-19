# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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
"""Unit tests for exponential backoff retrying."""

import time
import unittest
import apiclient.errors
from dsub.providers import google_base
import parameterized


def current_time_ms():
  return int(round(time.time() * 1000))


class GoogleApiMock(object):

  def __init__(self, exception_list):
    self.exception_list = exception_list
    self.retry_counter = 0

  def execute(self):
    if self.retry_counter < len(self.exception_list):
      e = self.exception_list[self.retry_counter]
      self.retry_counter += 1

      raise e

    return


class ResponseMock(object):

  def __init__(self, status, reason):
    self.status = status
    self.reason = reason


class TestRetrying(unittest.TestCase):

  def test_success(self):
    exception_list = []
    api_wrapper_to_test = google_base.Api()
    mock_api_object = GoogleApiMock(exception_list)
    start = current_time_ms()
    api_wrapper_to_test.execute(mock_api_object)
    # No expected retries, and no expected wait time
    self.assertEqual(mock_api_object.retry_counter, 0)
    self.assertLess(current_time_ms() - start, 500)

  @parameterized.parameterized.expand(
      [(error_code,)
       for error_code in list(google_base.TRANSIENT_HTTP_ERROR_CODES) +
       list(google_base.HTTP_AUTH_ERROR_CODES)])
  def test_retry_once(self, error_code):
    exception_list = [
        apiclient.errors.HttpError(
            ResponseMock(error_code, None), b'test_exception'),
    ]
    api_wrapper_to_test = google_base.Api()
    mock_api_object = GoogleApiMock(exception_list)
    start = current_time_ms()
    api_wrapper_to_test.execute(mock_api_object)
    # Expected to retry once, for about 1 second
    self.assertEqual(mock_api_object.retry_counter, 1)
    self.assertGreaterEqual(current_time_ms() - start, 1000)
    self.assertLess(current_time_ms() - start, 1500)

  def test_auth_failure(self):
    exception_list = [
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(401, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(401, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
    ]
    api_wrapper_to_test = google_base.Api()
    mock_api_object = GoogleApiMock(exception_list)
    # We don't want to retry auth errors as aggressively, so we expect
    # this exception to be raised after 4 retries,
    # for a total of 1 + 2 + 4 + 8 = 15 seconds
    start = current_time_ms()
    with self.assertRaises(apiclient.errors.HttpError):
      api_wrapper_to_test.execute(mock_api_object)
    self.assertGreaterEqual(current_time_ms() - start, 15000)
    self.assertLess(current_time_ms() - start, 15500)

  def test_transient_retries(self):
    exception_list = [
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(503, None), b'test_exception'),
    ]
    api_wrapper_to_test = google_base.Api()
    mock_api_object = GoogleApiMock(exception_list)
    start = current_time_ms()
    api_wrapper_to_test.execute(mock_api_object)
    # Expected to retry twice, for a total of 1 + 2 = 3 seconds
    self.assertEqual(mock_api_object.retry_counter, 2)
    self.assertGreaterEqual(current_time_ms() - start, 3000)
    self.assertLess(current_time_ms() - start, 3500)


if __name__ == '__main__':
  unittest.main()
