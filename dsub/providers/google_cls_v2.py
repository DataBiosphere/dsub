# Copyright 2019 Verily Life Sciences Inc. All Rights Reserved.
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
"""Provider for running jobs on Google Cloud Platform.

This module implements job creation, listing, and canceling using the
Google Cloud Life Sciences Pipelines and Operations APIs v2beta.
"""

from . import google_v2_base
from . import google_v2_versions

_PROVIDER_NAME = 'google-cls-v2'


class GoogleCLSV2JobProvider(google_v2_base.GoogleV2JobProviderBase):
  """dsub provider implementation managing Jobs on Google Cloud."""

  def __init__(self, dry_run, project, location, credentials=None):
    super(GoogleCLSV2JobProvider,
          self).__init__(_PROVIDER_NAME, google_v2_versions.V2BETA, credentials,
                         project, dry_run)

    self._location = location

  def _get_pipeline_regions(self, regions, zones):
    """Returns the list of regions to use for a pipeline request.

    If neither regions nor zones were specified for the pipeline, then use the
    v2beta location as the default region.

    Args:
      regions (str): A space separated list of regions to use for the pipeline.
      zones (str): A space separated list of zones to use for the pipeline.
    """

    if not regions and not zones:
      return [self._location]
    return regions or []

  def _pipelines_run_api(self, request):
    parent = 'projects/{}/locations/{}'.format(self._project, self._location)
    return self._service.projects().locations().pipelines().run(
        parent=parent, body=request)

  def _operations_list_api(self, ops_filter, page_token, page_size):
    name = 'projects/{}/locations/{}'.format(self._project, self._location)
    return self._service.projects().locations().operations().list(
        name=name, filter=ops_filter, pageToken=page_token, pageSize=page_size)

  def _operations_cancel_api_def(self):
    return self._service.projects().locations().operations().cancel

  def _batch_handler_def(self):
    """Returns a function object for the provider-specific batch handler."""

    # The batch endpoint currently only works for us-central1 requests.
    if self._location != 'us-central1':
      return google_v2_base.GoogleV2BatchHandler

    # The Lifesciences API provides a batch endpoint
    # (the Genomics v2alpha1 does not).
    #
    # This function returns the new_batch_http_request function, which the
    # caller can then use to create a BatchHttpRequest object.
    # The new_batch_http_request function is provided by the Google APIs
    # Python Client for batching requests destined for the batch endpoint.
    #
    # For documentation, see
    # https://googleapis.github.io/google-api-python-client/docs/dyn/lifesciences_v2beta.html#new_batch_http_request
    #
    # For example usage, see google_base.py (_cancel() and __cancel_batch()).

    return self._service.new_batch_http_request


if __name__ == '__main__':
  pass
