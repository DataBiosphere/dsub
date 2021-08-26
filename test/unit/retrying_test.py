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

import errno
import socket
import unittest

import apiclient.errors
from dsub.lib import retry_util
from dsub.providers import google_base
import fake_time
import google.auth
from mock import patch
import parameterized


def chronology():
  # Simulates the passing of time for fake_time.
  while True:
    yield 1  # Simulates 1 second passing.


def elapsed_time_in_seconds(fake_time_object):
  return int(round(fake_time_object.now()))


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
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      exception_list = []
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)

      api_wrapper_to_test.execute(mock_api_object)

      # No expected retries, and no expected wait time
      self.assertEqual(mock_api_object.retry_counter, 0)
      self.assertLess(elapsed_time_in_seconds(ft), 1)

  @parameterized.parameterized.expand(
      [(error_code,)
       for error_code in list(retry_util.TRANSIENT_HTTP_ERROR_CODES) +
       list(retry_util.HTTP_AUTH_ERROR_CODES)])
  def test_retry_once(self, error_code):
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      exception_list = [
          apiclient.errors.HttpError(
              ResponseMock(error_code, None), b'test_exception'),
      ]
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)

      api_wrapper_to_test.execute(mock_api_object)

      # Expected to retry once, for about 1 second
      self.assertEqual(mock_api_object.retry_counter, 1)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 1)
      self.assertLess(elapsed_time_in_seconds(ft), 1.5)

  def test_auth_failure(self):
    exception_list = [
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(401, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(401, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(403, None), b'test_exception'),
    ]
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)
      # We don't want to retry auth errors as aggressively, so we expect
      # this exception to be raised after 4 retries,
      # for a total of 1 + 2 + 4 + 8 = 15 seconds
      with self.assertRaises(apiclient.errors.HttpError):
        api_wrapper_to_test.execute(mock_api_object)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 15)
      self.assertLess(elapsed_time_in_seconds(ft), 15.5)

  def test_transient_retries(self):
    exception_list = [
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(503, None), b'test_exception'),
    ]
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)
      api_wrapper_to_test.execute(mock_api_object)
      # Expected to retry twice, for a total of 1 + 2 = 3 seconds
      self.assertEqual(mock_api_object.retry_counter, 2)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 3)
      self.assertLess(elapsed_time_in_seconds(ft), 3.5)

  def test_broken_pipe_retries(self):
    # To construct a BrokenPipeError, we pass errno.EPIPE (32) to OSError
    # See https://docs.python.org/3/library/exceptions.html#BaseException.args
    exception_list = [
        OSError(errno.EPIPE, 'broken pipe exception test'),
        OSError(errno.EPIPE, 'broken pipe exception test'),
    ]
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)
      api_wrapper_to_test.execute(mock_api_object)
      # Expected to retry twice, for a total of 1 + 2 = 3 seconds
      self.assertEqual(mock_api_object.retry_counter, 2)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 3)
      self.assertLess(elapsed_time_in_seconds(ft), 3.5)

  def test_socket_timeout(self):
    exception_list = [
        socket.timeout(),
        socket.timeout(),
    ]
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)
      api_wrapper_to_test.execute(mock_api_object)
      # Expected to retry twice, for a total of 1 + 2 = 3 seconds
      self.assertEqual(mock_api_object.retry_counter, 2)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 3)
      self.assertLess(elapsed_time_in_seconds(ft), 3.5)

  @parameterized.parameterized.expand([
      (apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
       'googleapiclient.errors.HttpError'),
      (google.auth.exceptions.RefreshError(),
       'google.auth.exceptions.RefreshError'),
      (socket.timeout(), 'socket.timeout'),
  ])
  def test_get_exception_type_string(self, exception, expected_type_string):
    actual_exception_string = retry_util.get_exception_type_string(exception)
    self.assertEqual(actual_exception_string, expected_type_string)

  def test_retry_then_succeed(self):
    exception_list = [
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
        apiclient.errors.HttpError(ResponseMock(500, None), b'test_exception'),
    ]
    ft = fake_time.FakeTime(chronology())
    with patch('time.sleep', new=ft.sleep):
      api_wrapper_to_test = google_base.Api()
      mock_api_object = GoogleApiMock(exception_list)
      api_wrapper_to_test.execute(mock_api_object)
      # Expected to retry 5 times,
      # for a total of 1 + 2 + 4 + 8 + 16 = 31 seconds
      # At the end we expect a recovery message to be emitted.
      self.assertEqual(mock_api_object.retry_counter, 5)
      self.assertGreaterEqual(elapsed_time_in_seconds(ft), 31)
      self.assertLess(elapsed_time_in_seconds(ft), 31.5)


if __name__ == '__main__':
  unittest.main()
