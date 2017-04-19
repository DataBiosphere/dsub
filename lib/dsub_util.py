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

import io
import os
import pwd
from StringIO import StringIO
import sys

from apiclient import discovery
from apiclient.http import MediaIoBaseDownload
from oauth2client.client import GoogleCredentials


def print_error(msg):
  """Utility routine to emit messages to stderr."""
  print >> sys.stderr, msg


def get_default_user():
  return pwd.getpwuid(os.getuid())[0]


def _load_file_from_gcs(gcs_file_path, credentials=None):
  """Load context from a text file in gcs.

  Args:
    gcs_file_path: The target file path; should have the 'gs://' prefix.
    credentials: Optional credential to be used to load the file from gcs.

  Returns:
    The content of the text file as a string.
  """
  if credentials is None:
    credentials = GoogleCredentials.get_application_default()
  gcs_service = discovery.build('storage', 'v1', credentials=credentials)

  bucket_name, object_name = gcs_file_path[len('gs://'):].split('/', 1)
  req = gcs_service.objects().get_media(bucket=bucket_name, object=object_name)

  file_handle = io.BytesIO()
  downloader = MediaIoBaseDownload(file_handle, req, chunksize=1024 * 1024)
  done = False
  while not done:
    _, done = downloader.next_chunk()

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
