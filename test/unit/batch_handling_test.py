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
"""Unit tests for batch handling exceptions."""

import unittest
import apiclient.errors
from dsub.providers import google_v2_base


def callback_mock(request_id, response, exception):
  del request_id, response  # unused
  raise exception


class CancelMock(object):

  def __init__(self):
    pass

  def execute(self):
    raise apiclient.errors.HttpError(None, b'test_exception')


class TestBatchHandling(unittest.TestCase):

  def test_success(self):
    # Make a handler that executes a function that raises an http error,
    # make sure the handler catches the error correctly

    api_handler_to_test = google_v2_base.GoogleV2BatchHandler(callback_mock)
    api_handler_to_test.add(CancelMock(), 'test_request_id')
    with self.assertRaises(apiclient.errors.HttpError):
      api_handler_to_test.execute()


if __name__ == '__main__':
  unittest.main()
