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

from __future__ import print_function

from contextlib import contextmanager
import fnmatch
import io
import os
import pwd
from StringIO import StringIO
import sys

from apiclient import discovery
from apiclient import errors
from apiclient.http import MediaIoBaseDownload
import oauth2client.client
from oauth2client.client import GoogleCredentials
import retrying


# this is the Job ID for jobs that are skipped.
NO_JOB = 'NO_JOB'


def replace_timezone(dt, tz):
  # pylint: disable=g-tzinfo-replace
  return dt.replace(tzinfo=tz)


class _Printer(object):
  """File-like stream object that redirects stdout to a file object."""

  def __init__(self, fileobj):
    self._actual_stdout = sys.stdout
    self._fileobj = fileobj

  def write(self, buf):
    self._fileobj.write(buf)


@contextmanager
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


def _get_storage_service(credentials):
  """Get a storage client using the provided credentials or defaults."""
  if credentials is None:
    credentials = GoogleCredentials.get_application_default()
  return discovery.build('storage', 'v1', credentials=credentials)


def _retry_download_check(exception):
  """Return True if we should retry, False otherwise."""
  print_error('Exception during download: %s' % str(exception))
  return isinstance(exception, oauth2client.client.HttpAccessTokenRefreshError)


# Exponential backoff retrying downloads of GCS object chunks.
# Maximum 23 retries.
# Wait 1, 2, 4 ... 64, 64, 64... seconds.
@retrying.retry(stop_max_attempt_number=23,
                retry_on_exception=_retry_download_check,
                wait_exponential_multiplier=1000, wait_exponential_max=64000)
def _downloader_next_chunk(downloader):
  return downloader.next_chunk()


def _load_file_from_gcs(gcs_file_path, credentials=None):
  """Load context from a text file in gcs.

  Args:
    gcs_file_path: The target file path; should have the 'gs://' prefix.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    The content of the text file as a string.
  """
  gcs_service = _get_storage_service(credentials)

  bucket_name, object_name = gcs_file_path[len('gs://'):].split('/', 1)
  request = gcs_service.objects().get_media(
      bucket=bucket_name, object=object_name)

  file_handle = io.BytesIO()
  downloader = MediaIoBaseDownload(file_handle, request, chunksize=1024 * 1024)
  done = False
  while not done:
    _, done = _downloader_next_chunk(downloader)

  return StringIO(file_handle.getvalue())


def load_file(file_path, credentials=None):
  """Load a file from either local or gcs.

  Args:
    file_path: The target file path, which should have the prefix 'gs://' if
               to be loaded from gcs.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    A python File object if loading file from local or a StringIO object if
    loading from gcs.
  """
  if file_path.startswith('gs://'):
    return _load_file_from_gcs(file_path, credentials)
  else:
    return open(file_path, 'r')


def _file_exists_in_gcs(gcs_file_path, credentials=None):
  """Check whether the file exists, in GCS.

  Args:
    gcs_file_path: The target file path; should have the 'gs://' prefix.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    True if the file's there.
  """
  gcs_service = _get_storage_service(credentials)

  bucket_name, object_name = gcs_file_path[len('gs://'):].split('/', 1)
  request = gcs_service.objects().get(
      bucket=bucket_name, object=object_name, projection='noAcl')
  try:
    request.execute()
    return True
  except errors.HttpError:
    return False


def file_exists(file_path, credentials=None):
  """Check whether the file exists, on local disk or GCS.

  Args:
    file_path: The target file path; should have the 'gs://' prefix if in gcs.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    True if the file's there.
  """
  if file_path.startswith('gs://'):
    return _file_exists_in_gcs(file_path, credentials)
  else:
    return os.path.isfile(file_path)


def _prefix_exists_in_gcs(gcs_prefix, credentials=None):
  """Check whether there is a GCS object whose name starts with the prefix.

  Since GCS doesn't actually have folders, this is how we check instead.

  Args:
    gcs_prefix: The path; should start with 'gs://'.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    True if the prefix matches at least one object in GCS.

  Raises:
    errors.HttpError: if it can't talk to the server
  """
  gcs_service = _get_storage_service(credentials)

  bucket_name, prefix = gcs_prefix[len('gs://'):].split('/', 1)
  # documentation in
  # https://cloud.google.com/storage/docs/json_api/v1/objects/list
  request = gcs_service.objects().list(
      bucket=bucket_name, prefix=prefix, maxResults=1)
  response = request.execute()
  return response.get('items', None)


def folder_exists(folder_path, credentials=None):
  if folder_path.startswith('gs://'):
    return _prefix_exists_in_gcs(folder_path.rstrip('/') + '/', credentials)
  else:
    return os.path.isdir(folder_path)


def simple_pattern_exists_in_gcs(file_pattern, credentials=None):
  """True iff an object exists matching the input GCS pattern.

  The GCS pattern must be a full object reference or a "simple pattern" that
  conforms to the dsub input and output parameter restrictions:

    * No support for **, ? wildcards or [] character ranges
    * Wildcards may only appear in the file name

  Args:
    file_pattern: eg. 'gs://foo/ba*'
    credentials: Optional credential to be used to load the file from gcs.

  Raises:
    ValueError: if file_pattern breaks the rules.

  Returns:
    True iff a file exists that matches that pattern.
  """
  if '*' not in file_pattern:
    return _file_exists_in_gcs(file_pattern, credentials)
  if not file_pattern.startswith('gs://'):
    raise ValueError('file name must start with gs://')
  gcs_service = _get_storage_service(credentials)
  bucket_name, prefix = file_pattern[len('gs://'):].split('/', 1)
  if '*' in bucket_name:
    raise ValueError('Wildcards may not appear in the bucket name')
  # There is a '*' in prefix because we checked there's one in file_pattern
  # and there isn't one in bucket_name. Hence it must be in prefix.
  assert '*' in prefix
  prefix_no_wildcard = prefix[:prefix.index('*')]
  request = gcs_service.objects().list(
      bucket=bucket_name, prefix=prefix_no_wildcard)
  response = request.execute()
  if 'items' not in response:
    return False
  items_list = [i['name'] for i in response['items']]
  return any(fnmatch.fnmatch(i, prefix) for i in items_list)


def outputs_are_present(outputs):
  """True if each output contains at least one file or no output specified."""
  # outputs are OutputFileParam (see param_util.py)

  # If outputs contain a pattern, then there is no way for `dsub` to verify
  # that *all* output is present. The best that `dsub` can do is to verify
  # that *some* output was created for each such parameter.
  for o in outputs:
    if not o.value:
      continue
    if o.recursive:
      if not folder_exists(o.value):
        return False
    else:
      if not simple_pattern_exists_in_gcs(o.value):
        return False
  return True
