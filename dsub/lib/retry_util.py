# Lint as: python2, python3
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

from __future__ import print_function

import datetime
import socket
import ssl
import sys

import googleapiclient.errors
from httplib2 import ServerNotFoundError
import six.moves.http_client

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


def _print_error(msg):
  """Utility routine to emit messages to stderr."""
  print(msg, file=sys.stderr)


def _print_retry_error(exception, verbose):
  """Prints an error message if appropriate."""
  if not verbose:
    return

  now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
  try:
    status_code = exception.resp.status
  except AttributeError:
    status_code = ''

  _print_error('{}: Caught exception {} {}'.format(now,
                                                   type(exception).__name__,
                                                   status_code))
  _print_error('{}: This request is being retried'.format(now))


def retry_api_check(exception, verbose):
  """Return True if we should retry.

  False otherwise.

  Args:
    exception: An exception to test for transience.
    verbose: If true, output retry messages

  Returns:
    True if we should retry. False otherwise.
  """
  if isinstance(exception, googleapiclient.errors.HttpError):
    if exception.resp.status in TRANSIENT_HTTP_ERROR_CODES:
      _print_retry_error(exception, verbose)
      return True

  if isinstance(exception, socket.error):
    if exception.errno in TRANSIENT_SOCKET_ERROR_CODES:
      _print_retry_error(exception, verbose)
      return True

  if isinstance(exception, google.auth.exceptions.RefreshError):
    _print_retry_error(exception, verbose)
    return True

  # For a given installation, this could be a permanent error, but has only
  # been observed as transient.
  if isinstance(exception, ssl.SSLError):
    _print_retry_error(exception, verbose)
    return True

  # This has been observed as a transient error:
  #   ServerNotFoundError: Unable to find the server at genomics.googleapis.com
  if isinstance(exception, ServerNotFoundError):
    _print_retry_error(exception, verbose)
    return True

  # Observed to be thrown transiently from auth libraries which use httplib2
  # Use the one from six because httlib no longer exists in Python3
  # https://docs.python.org/2/library/httplib.html
  if isinstance(exception, six.moves.http_client.ResponseNotReady):
    _print_retry_error(exception, verbose)
    return True

  return False


def retry_api_check_quiet(exception):
  return retry_api_check(exception, False)


def retry_api_check_verbose(exception):
  return retry_api_check(exception, True)


def retry_auth_check(exception, verbose):
  """Specific check for auth error codes.

  Return True if we should retry.

  False otherwise.
  Args:
    exception: An exception to test for transience.
    verbose: If true, output retry messages

  Returns:
    True if we should retry. False otherwise.
  """
  if isinstance(exception, googleapiclient.errors.HttpError):
    if exception.resp.status in HTTP_AUTH_ERROR_CODES:
      _print_retry_error(exception, verbose)
      return True

  return False


def retry_auth_check_quiet(exception):
  return retry_auth_check(exception, False)


def retry_auth_check_verbose(exception):
  return retry_auth_check(exception, True)
