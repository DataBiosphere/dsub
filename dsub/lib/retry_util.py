# Lint as: python3
# Copyright 2020 Verily Life Sciences Inc. All Rights Reserved
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
"""Utility functions used for retry logic in dsub, dstat, ddel."""

import datetime
import http.client
import socket
import ssl
import sys

import googleapiclient.errors
from httplib2 import ServerNotFoundError
import tenacity

import google.auth

# Transient errors for the Google APIs should not cause them to fail.
# There are a set of HTTP and socket errors which we automatically retry.
#  429: too frequent polling
#  50x: backend error
TRANSIENT_HTTP_ERROR_CODES = frozenset([429, 500, 503, 504])

# Auth errors should be permanent errors that a user needs to go fix
# (enable a service, grant access on a resource).
# However we have seen them occur transiently, so let's retry them when we
# see them, but not as patiently.
HTTP_AUTH_ERROR_CODES = frozenset([401, 403])

# Socket error 32 (broken pipe) and 104 (connection reset) should
# also be retried
TRANSIENT_SOCKET_ERROR_CODES = frozenset([32, 104])

# The maximum number of attempts when retrying API errors (network, 500s, etc)
MAX_API_ATTEMPTS = 24

# The maximum number of attempts when retrying auth errors (refresh tokens,
# 401s, etc)
MAX_AUTH_ATTEMPTS = 5


def _print_error(msg):
  """Utility routine to emit messages to stderr."""
  print(msg, file=sys.stderr)


def _print_retry_error(attempt_number, max_attempts, exception):
  """Prints an error message if appropriate."""
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
  try:
    status_code = exception.resp.status
  except AttributeError:
    status_code = ''

  if attempt_number % 5 == 0:
    _print_error('{}: Caught exception {} {}'.format(
        now, get_exception_type_string(exception), status_code))
    _print_error('{}: This request is being retried (attempt {} of {}).'.format(
        now, attempt_number, max_attempts))


def get_exception_type_string(exception):
  """Returns the full path of the exception."""
  exception_type_string = str(type(exception))

  # This is expected to look something like
  # "<class 'google.auth.exceptions.RefreshError'>"
  if exception_type_string.startswith(
      "<class '") and exception_type_string.endswith("'>"):
    # Slice off the <class ''> parts
    return exception_type_string[len("<class '"):-len("'>")]
  else:
    # If the exception type looks different than expected,
    # just print out the whole type.
    return exception_type_string


def retry_api_check(retry_state: tenacity.RetryCallState) -> bool:
  """Return True if we should retry.

  False otherwise.

  Args:
    retry_state: A retry state including exception to test for transience.

  Returns:
    True if we should retry. False otherwise.
  """
  exception = retry_state.outcome.exception()
  attempt_number = retry_state.attempt_number
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

  if isinstance(exception, googleapiclient.errors.HttpError):
    if exception.resp.status in TRANSIENT_HTTP_ERROR_CODES:
      _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
      return True

  if isinstance(exception, socket.error):
    if exception.errno in TRANSIENT_SOCKET_ERROR_CODES:
      _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
      return True

  if isinstance(exception, socket.timeout):
    _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
    return True

  if isinstance(exception, google.auth.exceptions.RefreshError):
    _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
    return True

  # For a given installation, this could be a permanent error, but has only
  # been observed as transient.
  if isinstance(exception, ssl.SSLError):
    _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
    return True

  # This has been observed as a transient error:
  #   ServerNotFoundError: Unable to find the server at genomics.googleapis.com
  if isinstance(exception, ServerNotFoundError):
    _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
    return True

  # Observed to be thrown transiently from auth libraries which use httplib2
  if isinstance(exception, http.client.ResponseNotReady):
    _print_retry_error(attempt_number, MAX_API_ATTEMPTS, exception)
    return True

  if not exception and attempt_number > 5:
    _print_error('{}: Retry SUCCEEDED'.format(now))

  return False


def retry_auth_check(retry_state: tenacity.RetryCallState) -> bool:
  """Specific check for auth error codes.

  Return True if we should retry.

  False otherwise.
  Args:
    retry_state: A retry state including exception to test for transience.

  Returns:
    True if we should retry. False otherwise.
  """
  exception = retry_state.outcome.exception()
  attempt_number = retry_state.attempt_number
  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

  if isinstance(exception, googleapiclient.errors.HttpError):
    if exception.resp.status in HTTP_AUTH_ERROR_CODES:
      _print_retry_error(attempt_number, MAX_AUTH_ATTEMPTS, exception)
      return True

  if not exception and attempt_number > 4:
    _print_error('{}: Retry SUCCEEDED'.format(now))

  return False


def on_give_up(retry_state: tenacity.RetryCallState) -> None:
  """Called after all retries failed.

  Simply outputs a message and re-raises.

  Args:
    retry_state: info about current retry invocation.

  Returns:
    None.
  """
  exception = retry_state.outcome.exception()
  attempt_number = retry_state.attempt_number
  _print_error('Giving up after {} attempts'.format(attempt_number))
  raise exception
