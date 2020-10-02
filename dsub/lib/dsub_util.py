# Lint as: python3
# Copyright 2016 Google Inc. All Rights Reserved.
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
"""Utility functions used by dsub, dstat, ddel."""

import contextlib
import fnmatch
import io
import os
import pwd
import sys
import warnings
from . import retry_util

import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import tenacity

import google.auth


# this is the Job ID for jobs that are skipped.
NO_JOB = 'NO_JOB'


def replace_timezone(dt, tz):
  # pylint: disable=g-tzinfo-replace
  return dt.replace(tzinfo=tz)


def datetime_is_timezone_aware(dt):
  # From datetime docs:
  # https://docs.python.org/3/library/datetime.html#determining-if-an-object-is-aware-or-naive
  return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


class _Printer(object):
  """File-like stream object that redirects stdout to a file object."""

  def __init__(self, fileobj):
    self._actual_stdout = sys.stdout
    self._fileobj = fileobj

  def write(self, buf):
    self._fileobj.write(buf)


@contextlib.contextmanager
def replace_print(fileobj=sys.stderr):
  """Sys.out replacer, by default with stderr.

  Use it like this:
  with replace_print_with(fileobj):
    print "hello"  # writes to the file
  print "done"  # prints to stdout

  Args:
    fileobj: a file object to replace stdout.

  Yields:
    The printer.
  """
  printer = _Printer(fileobj)

  previous_stdout = sys.stdout
  sys.stdout = printer
  try:
    yield printer
  finally:
    sys.stdout = previous_stdout


def print_error(msg):
  """Utility routine to emit messages to stderr."""
  print(msg, file=sys.stderr)


def get_os_user():
  """Returns the current OS user, this may be different from the dsub user."""
  return pwd.getpwuid(os.getuid())[0]


def tasks_to_job_ids(task_list):
  """Returns the set of job IDs for the given tasks."""
  return set([t.get_field('job-id') for t in task_list])


def compact_interval_string(value_list):
  """Compact a list of integers into a comma-separated string of intervals.

  Args:
    value_list: A list of sortable integers such as a list of numbers

  Returns:
    A compact string representation, such as "1-5,8,12-15"
  """

  if not value_list:
    return ''

  value_list.sort()

  # Start by simply building up a list of separate contiguous intervals
  interval_list = []
  curr = []
  for val in value_list:
    if curr and (val > curr[-1] + 1):
      interval_list.append((curr[0], curr[-1]))
      curr = [val]
    else:
      curr.append(val)

  if curr:
    interval_list.append((curr[0], curr[-1]))

  # For each interval collapse it down to "first, last" or just "first" if
  # if first == last.
  return ','.join([
      '{}-{}'.format(pair[0], pair[1]) if pair[0] != pair[1] else str(pair[0])
      for pair in interval_list
  ])


def get_storage_service(credentials):
  """Get a storage client using the provided credentials or defaults."""
  # dsub is not a server application, so it is ok to filter this warning.
  warnings.filterwarnings(
      'ignore', 'Your application has authenticated using end user credentials')
  if credentials is None:
    credentials, _ = google.auth.default()
  return googleapiclient.discovery.build(
      'storage', 'v1', credentials=credentials)


# Exponential backoff retrying downloads of GCS object chunks.
# Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_API_ATTEMPTS),
    retry=retry_util.retry_api_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=64),
    retry_error_callback=retry_util.on_give_up)
# For API errors dealing with auth, we want to retry, but not as often
# Maximum 4 retries. Wait 1, 2, 4, 8 seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_AUTH_ATTEMPTS),
    retry=retry_util.retry_auth_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=8),
    retry_error_callback=retry_util.on_give_up)
def _downloader_next_chunk(downloader):
  """Downloads the next chunk."""
  return downloader.next_chunk()


def _load_file_from_gcs(gcs_file_path, credentials=None):
  """Load context from a text file in gcs.

  Args:
    gcs_file_path: The target file path; should have the 'gs://' prefix.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    The content of the text file as a string.
  """
  gcs_service = get_storage_service(credentials)

  bucket_name, object_name = gcs_file_path[len('gs://'):].split('/', 1)
  request = gcs_service.objects().get_media(
      bucket=bucket_name, object=object_name)

  file_handle = io.BytesIO()
  downloader = googleapiclient.http.MediaIoBaseDownload(
      file_handle, request, chunksize=1024 * 1024)
  done = False
  while not done:
    _, done = _downloader_next_chunk(downloader)
  filevalue = file_handle.getvalue()
  if not isinstance(filevalue, str):
    filevalue = filevalue.decode()
  return filevalue


def load_file(file_path, credentials=None):
  """Load a file from either local or gcs.

  Args:
    file_path: The target file path, which should have the prefix 'gs://' if
               to be loaded from gcs.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    The contents of the file as a string.
  """
  if file_path.startswith('gs://'):
    return _load_file_from_gcs(file_path, credentials)
  else:
    with open(file_path, 'r') as f:
      return f.read()


# Exponential backoff retrying downloads of GCS object chunks.
# Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_API_ATTEMPTS),
    retry=retry_util.retry_api_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=64),
    retry_error_callback=retry_util.on_give_up)
# For API errors dealing with auth, we want to retry, but not as often
# Maximum 4 retries. Wait 1, 2, 4, 8 seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_AUTH_ATTEMPTS),
    retry=retry_util.retry_auth_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=8),
    retry_error_callback=retry_util.on_give_up)
def _file_exists_in_gcs(gcs_file_path, credentials=None, storage_service=None):
  """Check whether the file exists, in GCS.

  Args:
    gcs_file_path: The target file path; should have the 'gs://' prefix.
    credentials: Optional credential to be used to load the file from gcs.
    storage_service: GCS API service object.

  Returns:
    True if the file's there.
  """
  if storage_service is None:
    storage_service = get_storage_service(credentials)

  bucket_name, object_name = gcs_file_path[len('gs://'):].split('/', 1)
  request = storage_service.objects().get(
      bucket=bucket_name, object=object_name, projection='noAcl')
  try:
    request.execute()
    return True
  except googleapiclient.errors.HttpError:
    return False


# Exponential backoff retrying downloads of GCS object chunks.
# Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_API_ATTEMPTS),
    retry=retry_util.retry_api_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=64),
    retry_error_callback=retry_util.on_give_up)
# For API errors dealing with auth, we want to retry, but not as often
# Maximum 4 retries. Wait 1, 2, 4, 8 seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_AUTH_ATTEMPTS),
    retry=retry_util.retry_auth_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=8),
    retry_error_callback=retry_util.on_give_up)
def _prefix_exists_in_gcs(gcs_prefix, credentials=None, storage_service=None):
  """Check whether there is a GCS object whose name starts with the prefix.

  Since GCS doesn't actually have folders, this is how we check instead.

  Args:
    gcs_prefix: The path; should start with 'gs://'.
    credentials: Optional credential to be used to load the file from gcs.
    storage_service: GCS API service object.

  Returns:
    True if the prefix matches at least one object in GCS.

  Raises:
    googleapiclient.errors.HttpError: if it can't talk to the server
  """
  if storage_service is None:
    storage_service = get_storage_service(credentials)

  bucket_name, prefix = gcs_prefix[len('gs://'):].split('/', 1)
  # documentation in
  # https://cloud.google.com/storage/docs/json_api/v1/objects/list
  request = storage_service.objects().list(
      bucket=bucket_name, prefix=prefix, maxResults=1)
  response = request.execute()
  return response.get('items', None)


def folder_exists(folder_path, credentials=None, storage_service=None):
  if folder_path.startswith('gs://'):
    return _prefix_exists_in_gcs(
        folder_path.rstrip('/') + '/', credentials, storage_service)
  else:
    return os.path.isdir(folder_path)


# Exponential backoff retrying downloads of GCS object chunks.
# Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_API_ATTEMPTS),
    retry=retry_util.retry_api_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=64),
    retry_error_callback=retry_util.on_give_up)
# For API errors dealing with auth, we want to retry, but not as often
# Maximum 4 retries. Wait 1, 2, 4, 8 seconds.
@tenacity.retry(
    stop=tenacity.stop_after_attempt(retry_util.MAX_AUTH_ATTEMPTS),
    retry=retry_util.retry_auth_check,
    wait=tenacity.wait_exponential(multiplier=0.5, max=8),
    retry_error_callback=retry_util.on_give_up)
def simple_pattern_exists_in_gcs(file_pattern,
                                 credentials=None,
                                 storage_service=None):
  """True iff an object exists matching the input GCS pattern.

  The GCS pattern must be a full object reference or a "simple pattern" that
  conforms to the dsub input and output parameter restrictions:

    * No support for **, ? wildcards or [] character ranges
    * Wildcards may only appear in the file name

  Args:
    file_pattern: eg. 'gs://foo/ba*'
    credentials: Optional credential to be used to load the file from gcs.
    storage_service: GCS API service object.

  Raises:
    ValueError: if file_pattern breaks the rules.

  Returns:
    True iff a file exists that matches that pattern.
  """
  if '*' not in file_pattern:
    return _file_exists_in_gcs(file_pattern, credentials, storage_service)
  if not file_pattern.startswith('gs://'):
    raise ValueError('file name must start with gs://')

  if storage_service is None:
    storage_service = get_storage_service(credentials)

  bucket_name, prefix = file_pattern[len('gs://'):].split('/', 1)
  if '*' in bucket_name:
    raise ValueError('Wildcards may not appear in the bucket name')
  # There is a '*' in prefix because we checked there's one in file_pattern
  # and there isn't one in bucket_name. Hence it must be in prefix.
  assert '*' in prefix
  prefix_no_wildcard = prefix[:prefix.index('*')]
  request = storage_service.objects().list(
      bucket=bucket_name, prefix=prefix_no_wildcard)
  response = request.execute()
  if 'items' not in response:
    return False
  items_list = [i['name'] for i in response['items']]
  return any(fnmatch.fnmatch(i, prefix) for i in items_list)


def outputs_are_present(outputs, storage_service=None):
  """True if each output contains at least one file or no output specified."""
  # outputs are OutputFileParam (see param_util.py)

  # If outputs contain a pattern, then there is no way for `dsub` to verify
  # that *all* output is present. The best that `dsub` can do is to verify
  # that *some* output was created for each such parameter.
  for o in outputs:
    if not o.value:
      continue
    if o.recursive:
      if not folder_exists(o.value, storage_service=storage_service):
        return False
    else:
      if not simple_pattern_exists_in_gcs(
          o.value, storage_service=storage_service):
        return False
  return True
