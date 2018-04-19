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
"""Base class for the google and google_v2 providers.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import socket
import sys

from .._dsub_version import DSUB_VERSION
import apiclient.discovery
import apiclient.errors
from ..lib import job_model
from oauth2client.client import GoogleCredentials
from oauth2client.client import HttpAccessTokenRefreshError
import retrying

# Transient errors for the Google APIs should not cause them to fail.
# There are a set of HTTP and socket errors which we automatically retry.
#  429: too frequent polling
#  50x: backend error
TRANSIENT_HTTP_ERROR_CODES = set([429, 500, 503, 504])

# Socket error 104 (connection reset) should also be retried
TRANSIENT_SOCKET_ERROR_CODES = set([104])

# List of Compute Engine zones, which enables simple wildcard expansion.
# We could look this up dynamically, but new zones come online
# infrequently enough, this is easy to keep up with.
# Also - the Pipelines API may one day directly support zone wildcards.
#
# To refresh this list:
#   gcloud compute zones list --format='value(name)' \
#     | sort | awk '{ printf "    '\''%s'\'',\n", $1 }'
_ZONES = [
    'asia-east1-a',
    'asia-east1-b',
    'asia-east1-c',
    'asia-northeast1-a',
    'asia-northeast1-b',
    'asia-northeast1-c',
    'asia-south1-a',
    'asia-south1-b',
    'asia-south1-c',
    'asia-southeast1-a',
    'asia-southeast1-b',
    'australia-southeast1-a',
    'australia-southeast1-b',
    'australia-southeast1-c',
    'europe-west1-b',
    'europe-west1-c',
    'europe-west1-d',
    'europe-west2-a',
    'europe-west2-b',
    'europe-west2-c',
    'europe-west3-a',
    'europe-west3-b',
    'europe-west3-c',
    'europe-west4-a',
    'europe-west4-b',
    'europe-west4-c',
    'northamerica-northeast1-a',
    'northamerica-northeast1-b',
    'northamerica-northeast1-c',
    'southamerica-east1-a',
    'southamerica-east1-b',
    'southamerica-east1-c',
    'us-central1-a',
    'us-central1-b',
    'us-central1-c',
    'us-central1-f',
    'us-east1-b',
    'us-east1-c',
    'us-east1-d',
    'us-east4-a',
    'us-east4-b',
    'us-east4-c',
    'us-west1-a',
    'us-west1-b',
    'us-west1-c',
]


def _print_error(msg):
  """Utility routine to emit messages to stderr."""
  print(msg, file=sys.stderr)


def get_zones(input_list):
  """Returns a list of zones based on any wildcard input.

  This function is intended to provide an easy method for producing a list
  of desired zones for a pipeline to run in.

  The Pipelines API default zone list is "any zone". The problem with
  "any zone" is that it can lead to incurring Cloud Storage egress charges
  if the GCE zone selected is in a different region than the GCS bucket.
  See https://cloud.google.com/storage/pricing#network-egress.

  A user with a multi-region US bucket would want to pipelines to run in
  a "us-*" zone.
  A user with a regional bucket in US would want to restrict pipelines to
  run in a zone in that region.

  Rarely does the specific zone matter for a pipeline.

  This function allows for a simple short-hand such as:
     [ "us-*" ]
     [ "us-central1-*" ]
  These examples will expand out to the full list of US and us-central1 zones
  respectively.

  Args:
    input_list: list of zone names/patterns

  Returns:
    A list of zones, with any wildcard zone specifications expanded.
  """
  if not input_list:
    return []

  output_list = []

  for zone in input_list:
    if zone.endswith('*'):
      prefix = zone[:-1]
      output_list.extend([z for z in _ZONES if z.startswith(prefix)])
    else:
      output_list.append(zone)

  return output_list


class Label(job_model.LabelParam):
  """Name/value label metadata for a Google Genomics pipeline.

  Attributes:
    name (str): the label name.
    value (str): the label value (optional).
  """
  _allow_reserved_keys = True
  __slots__ = ()


def build_pipeline_labels(job_metadata, task_metadata):
  """Build a dictionary of standard job and task labels."""
  labels = {
      Label(name, job_metadata[name])
      for name in ['job-name', 'job-id', 'user-id', 'dsub-version']
  }

  if task_metadata.get('task-id') is not None:
    labels.add(Label('task-id', 'task-%d' % task_metadata.get('task-id')))

  return labels


def prepare_job_metadata(script, job_name, user_id, create_time):
  """Returns a dictionary of metadata fields for the job."""

  # The name of the pipeline gets set into the ephemeralPipeline.name as-is.
  # The default name of the pipeline is the script name
  # The name of the job is derived from the job_name and gets set as a
  # 'job-name' label (and so the value must be normalized).
  if job_name:
    pipeline_name = job_name
    job_name_value = job_model.convert_to_label_chars(job_name)
  else:
    pipeline_name = os.path.basename(script)
    job_name_value = job_model.convert_to_label_chars(
        pipeline_name.split('.', 1)[0])

  # The user-id will get set as a label
  user_id = job_model.convert_to_label_chars(user_id)

  # Now build the job-id. We want the job-id to be expressive while also
  # having a low-likelihood of collisions.
  #
  # For expressiveness, we:
  # * use the job name (truncated at 10 characters).
  # * insert the user-id
  # * add a datetime value
  # To have a high likelihood of uniqueness, the datetime value is out to
  # hundredths of a second.
  #
  # The full job-id is:
  #   <job-name>--<user-id>--<timestamp>
  job_id = '%s--%s--%s' % (job_name_value[:10], user_id,
                           create_time.strftime('%y%m%d-%H%M%S-%f')[:16])

  # Standard version is MAJOR.MINOR(.PATCH). This will convert the version
  # string to "vMAJOR-MINOR(-PATCH)". Example; "0.1.0" -> "v0-1-0".
  version = job_model.convert_to_label_chars('v%s' % DSUB_VERSION)
  return {
      'pipeline-name': pipeline_name,
      'job-name': job_name_value,
      'job-id': job_id,
      'user-id': user_id,
      'dsub-version': version,
  }


def retry_api_check(exception):
  """Return True if we should retry. False otherwise.

  Args:
    exception: An exception to test for transience.

  Returns:
    True if we should retry. False otherwise.
  """
  _print_error('Exception %s: %s' % (type(exception).__name__, str(exception)))

  if isinstance(exception, apiclient.errors.HttpError):
    if exception.resp.status in TRANSIENT_HTTP_ERROR_CODES:
      return True

  if isinstance(exception, socket.error):
    if exception.errno in TRANSIENT_SOCKET_ERROR_CODES:
      return True

  if isinstance(exception, HttpAccessTokenRefreshError):
    return True

  return False


# Exponential backoff retrying API discovery.
# Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
@retrying.retry(
    stop_max_attempt_number=23,
    retry_on_exception=retry_api_check,
    wait_exponential_multiplier=1000,
    wait_exponential_max=64000)
def setup_service(api_name, api_version, credentials=None):
  """Configures genomics API client.

  Args:
    api_name: Name of the Google API (for example: "genomics")
    api_version: Version of the API (for example: "v2alpha1")
    credentials: Credentials to be used for the gcloud API calls.

  Returns:
    A configured Google Genomics API client with appropriate credentials.
  """
  if not credentials:
    credentials = GoogleCredentials.get_application_default()
  return apiclient.discovery.build(
      api_name, api_version, credentials=credentials)


class Api(object):

  # Exponential backoff retrying API execution.
  # Maximum 23 retries.  Wait 1, 2, 4 ... 64, 64, 64... seconds.
  @staticmethod
  @retrying.retry(
      stop_max_attempt_number=23,
      retry_on_exception=retry_api_check,
      wait_exponential_multiplier=1000,
      wait_exponential_max=64000)
  def execute(api):
    return api.execute()


if __name__ == '__main__':
  pass
