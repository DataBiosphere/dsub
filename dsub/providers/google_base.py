# Lint as: python3
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
"""Base module for the google_v2 and google_cls_v2 providers."""

# pylint: disable=g-tzinfo-datetime
import datetime
import io
import json
import re
import warnings

import google.auth
from google.oauth2 import service_account
import googleapiclient.discovery
import googleapiclient.errors
from ..lib import job_model
from ..lib import retry_util
import pytz
import tenacity


# The google v1 provider directly added the bigquery scope, but the v1alpha2
# API automatically added:
# - https://www.googleapis.com/auth/compute
# - https://www.googleapis.com/auth/devstorage.full_control
# - https://www.googleapis.com/auth/genomics
# - https://www.googleapis.com/auth/logging.write
# - https://www.googleapis.com/auth/monitoring.write
#
# With the addition of the google v2 provider, we explicitly set all of these
# scopes such that existing user code continues to work.
# Note that with the API (for both v2 and cls_v2 provider), the
# `https://www.googleapis.com/auth/cloud-platform` scope is automatically added.
# See
# https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run#serviceaccount
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/compute',
    'https://www.googleapis.com/auth/devstorage.full_control',
    'https://www.googleapis.com/auth/genomics',
    'https://www.googleapis.com/auth/logging.write',
    'https://www.googleapis.com/auth/monitoring.write',
]


# When attempting to cancel an operation that is already completed
# (succeeded, failed, or canceled), the response will include:
# "error": {
#    "code": 400,
#    "status": "FAILED_PRECONDITION",
# }
FAILED_PRECONDITION_CODE = 400
FAILED_PRECONDITION_STATUS = 'FAILED_PRECONDITION'

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
    'asia-east2-a',
    'asia-east2-b',
    'asia-east2-c',
    'asia-northeast1-a',
    'asia-northeast1-b',
    'asia-northeast1-c',
    'asia-northeast2-a',
    'asia-northeast2-b',
    'asia-northeast2-c',
    'asia-northeast3-a',
    'asia-northeast3-b',
    'asia-northeast3-c',
    'asia-south1-a',
    'asia-south1-b',
    'asia-south1-c',
    'asia-southeast1-a',
    'asia-southeast1-b',
    'asia-southeast1-c',
    'asia-southeast2-a',
    'asia-southeast2-b',
    'asia-southeast2-c',
    'australia-southeast1-a',
    'australia-southeast1-b',
    'australia-southeast1-c',
    'europe-north1-a',
    'europe-north1-b',
    'europe-north1-c',
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
    'europe-west6-a',
    'europe-west6-b',
    'europe-west6-c',
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
    'us-west2-a',
    'us-west2-b',
    'us-west2-c',
    'us-west3-a',
    'us-west3-b',
    'us-west3-c',
    'us-west4-a',
    'us-west4-b',
    'us-west4-c',
]


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


def build_pipeline_labels(job_metadata, task_metadata, task_id_pattern=None):
  """Build a set() of standard job and task labels.

  Args:
    job_metadata: Job metadata, such as job-id, job-name, and user-id.
    task_metadata: Task metadata, such as the task-id.
    task_id_pattern: A pattern for the task-id value, such as "task-%d"; the
      original google label values could not be strictly numeric, so "task-"
      was prepended.

  Returns:
    A set of standard dsub Label() objects to attach to a pipeline.
  """
  labels = {
      Label(name, job_metadata[name])
      for name in ['job-name', 'job-id', 'user-id', 'dsub-version']
  }

  task_id = task_metadata.get('task-id')
  if task_id is not None:  # Check for None (as 0 is conceivably valid)
    if task_id_pattern:
      task_id = task_id_pattern % task_id
    labels.add(Label('task-id', str(task_id)))

  task_attempt = task_metadata.get('task-attempt')
  if task_attempt is not None:
    labels.add(Label('task-attempt', str(task_attempt)))

  return labels


def prepare_query_label_value(labels):
  """Converts the label strings to contain label-appropriate characters.

  Args:
    labels: A set of strings to be converted.

  Returns:
    A list of converted strings.
  """
  if not labels:
    return None
  return [job_model.convert_to_label_chars(label) for label in labels]


def parse_rfc3339_utc_string(rfc3339_utc_string):
  """Converts a datestamp from RFC3339 UTC to a datetime.

  Args:
    rfc3339_utc_string: a datetime string in RFC3339 UTC "Zulu" format

  Returns:
    A datetime.
  """

  # The timestamp from the Google Operations are all in RFC3339 format, but
  # they are sometimes formatted to millisconds, microseconds, sometimes
  # nanoseconds, and sometimes only seconds:
  # * 2016-11-14T23:05:56Z
  # * 2016-11-14T23:05:56.010Z
  # * 2016-11-14T23:05:56.010429Z
  # * 2016-11-14T23:05:56.010429380Z
  m = re.match(r'(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).?(\d*)Z',
               rfc3339_utc_string)

  # It would be unexpected to get a different date format back from Google.
  # If we raise an exception here, we can break people completely.
  # Instead, let's just return None and people can report that some dates
  # are not showing up.
  # We might reconsider this approach in the future; it was originally
  # established when dates were only used for display.
  if not m:
    return None

  groups = m.groups()
  if len(groups[6]) not in (0, 3, 6, 9):
    return None

  # Create a UTC datestamp from parsed components
  # 1- Turn components 0-5 from strings to integers
  # 2- If the last component does not exist, set it to 0.
  #    If it does exist, make sure to interpret it as milliseconds.
  g = [int(val) for val in groups[:6]]

  fraction = groups[6]
  if not fraction:
    micros = 0
  elif len(fraction) == 3:
    micros = int(fraction) * 1000
  elif len(fraction) == 6:
    micros = int(fraction)
  elif len(fraction) == 9:
    # When nanoseconds are provided, we round
    micros = int(round(int(fraction) // 1000))
  else:
    assert False, 'Fraction length not 0, 6, or 9: {}'.len(fraction)

  try:
    return datetime.datetime(
        g[0], g[1], g[2], g[3], g[4], g[5], micros, tzinfo=pytz.utc)
  except ValueError as e:
    assert False, 'Could not parse RFC3339 datestring: {} exception: {}'.format(
        rfc3339_utc_string, e)


def get_operation_full_job_id(op):
  """Returns the job-id or job-id.task-id for the operation."""
  job_id = op.get_field('job-id')
  task_id = op.get_field('task-id')
  if task_id:
    return '%s.%s' % (job_id, task_id)
  else:
    return job_id


def _cancel_batch(batch_fn, cancel_fn, ops):
  """Cancel a batch of operations.

  Args:
    batch_fn: API-specific batch function.
    cancel_fn: API-specific cancel function.
    ops: A list of operations to cancel.

  Returns:
    A list of operations canceled and a list of error messages.
  """

  # We define an inline callback which will populate a list of
  # successfully canceled operations as well as a list of operations
  # which were not successfully canceled.

  canceled = []
  failed = []

  def handle_cancel_response(request_id, response, exception):
    """Callback for the cancel response."""
    del response  # unused

    if exception:
      # We don't generally expect any failures here, except possibly trying
      # to cancel an operation that is already canceled or finished.
      #
      # If the operation is already finished, provide a clearer message than
      # "error 400: Bad Request".

      msg = 'error %s: %s' % (exception.resp.status, exception.resp.reason)
      if exception.resp.status == FAILED_PRECONDITION_CODE:
        detail = json.loads(exception.content)
        status = detail.get('error', {}).get('status')
        if status == FAILED_PRECONDITION_STATUS:
          msg = 'Not running'

      failed.append({'name': request_id, 'msg': msg})
    else:
      canceled.append({'name': request_id})

    return

  # Set up the batch object
  batch = batch_fn(callback=handle_cancel_response)

  # The callback gets a "request_id" which is the operation name.
  # Build a dict such that after the callback, we can lookup the operation
  # objects by name
  ops_by_name = {}
  for op in ops:
    op_name = op.get_field('internal-id')
    ops_by_name[op_name] = op
    batch.add(cancel_fn(name=op_name, body={}), request_id=op_name)

  # Cancel the operations
  batch.execute()

  # Iterate through the canceled and failed lists to build our return lists
  canceled_ops = [ops_by_name[op['name']] for op in canceled]
  error_messages = []
  for fail in failed:
    op = ops_by_name[fail['name']]
    error_messages.append("Error canceling '%s': %s" %
                          (get_operation_full_job_id(op), fail['msg']))

  return canceled_ops, error_messages


def cancel(batch_fn, cancel_fn, ops):
  """Cancel operations.

  Args:
    batch_fn: API-specific batch function.
    cancel_fn: API-specific cancel function.
    ops: A list of operations to cancel.

  Returns:
    A list of operations canceled and a list of error messages.
  """

  # Canceling many operations one-by-one can be slow.
  # The Pipelines API doesn't directly support a list of operations to cancel,
  # but the requests can be performed in batch.

  canceled_ops = []
  error_messages = []

  max_batch = 256
  total_ops = len(ops)
  for first_op in range(0, total_ops, max_batch):
    batch_canceled, batch_messages = _cancel_batch(
        batch_fn, cancel_fn, ops[first_op:first_op + max_batch])
    canceled_ops.extend(batch_canceled)
    error_messages.extend(batch_messages)

  return canceled_ops, error_messages


# Exponential backoff retrying API discovery.
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
def setup_service(api_name, api_version, credentials=None):
  """Configures genomics API client.

  Args:
    api_name: Name of the Google API (for example: "genomics")
    api_version: Version of the API (for example: "v2alpha1")
    credentials: Credentials to be used for the gcloud API calls.

  Returns:
    A configured Google Genomics API client with appropriate credentials.
  """
  # dsub is not a server application, so it is ok to filter this warning.
  warnings.filterwarnings(
      'ignore', 'Your application has authenticated using end user credentials')
  if not credentials:
    credentials, _ = google.auth.default()
  return googleapiclient.discovery.build(
      api_name, api_version, credentials=credentials)


def credentials_from_service_account_info(credentials_file):
  with io.open(credentials_file, 'r', encoding='utf-8') as json_fi:
    credentials_info = json.load(json_fi)
  return service_account.Credentials.from_service_account_info(credentials_info)


class Api(object):
  """Wrapper around API execution with exponential backoff retries."""

  # Exponential backoff retrying API execution
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
  def execute(self, api):
    """Executes operation.

    Args:
      api: The base API object

    Returns:
       A response body object
    """
    return api.execute()


if __name__ == '__main__':
  pass
