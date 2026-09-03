# Copyright 2026 Verily Life Sciences Inc. All Rights Reserved.
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
"""Tests for the cloud-sdk image used by the Google providers.

The google-batch and google-cls-v2 providers run their localization,
delocalization, and logging steps in CLOUD_SDK_IMAGE. If that image is not
pullable, every job fails at runtime while the rest of the test suite stays
green, because no other test references the constant.

That is not hypothetical: gcr.io deletes numbered cloud-sdk tags after a year,
which broke every dsub release that pinned one
(https://github.com/DataBiosphere/dsub/issues/336).

This test resolves the image's manifest against the registry over the Docker
Registry HTTP API V2 and fails if it does not exist. It needs no credentials,
no Docker daemon, and no gcloud. Because the tag is parsed out of the constant
itself, the test follows any future edit to it, and so covers re-pinning to a
numbered tag, renaming the repository (cloud-sdk -> google-cloud-cli), and
retention deletions alike.

Set DSUB_SKIP_NETWORK_TESTS=1 to skip. The test also skips, rather than fails,
when the registry cannot be reached at all, or when it refuses to serve an
anonymous read, so that an offline run or a privately hosted image does not
report a false breakage. A registry that answers but reports the image missing
is always a failure.
"""

import importlib
import json
import os
import re
import unittest
from unittest import mock
import urllib.error
import urllib.parse
import urllib.request

from dsub.providers import google_utils

# Ask for every manifest media type the cloud-sdk image might be published as.
# A multi-architecture image answers with an index rather than a manifest, and a
# registry may return 404 for a media type it was not asked about.
_ACCEPT_MANIFEST_TYPES = ', '.join([
    'application/vnd.docker.distribution.manifest.list.v2+json',
    'application/vnd.docker.distribution.manifest.v2+json',
    'application/vnd.oci.image.index.v1+json',
    'application/vnd.oci.image.manifest.v1+json',
])

_TIMEOUT_SECONDS = 30


def _split_image(image):
  """Splits a container image reference into registry, repository, reference.

  Args:
    image: An image reference, such as
      'gcr.io/google.com/cloudsdktool/cloud-sdk:slim'.

  Returns:
    A (registry, repository, reference) tuple, where reference is a tag or a
    'sha256:...' digest.
  """
  registry, _, remainder = image.partition('/')

  # A digest reference ('repo@sha256:...') takes precedence over a tag.
  if '@' in remainder:
    repository, _, reference = remainder.partition('@')
    return registry, repository, reference

  # A colon is only a tag separator if it appears in the last path segment;
  # otherwise the reference is implicitly 'latest'.
  if ':' in remainder.rsplit('/', 1)[-1]:
    repository, _, reference = remainder.rpartition(':')
    return registry, repository, reference

  return registry, remainder, 'latest'


def _head(url, headers):
  """Issues a HEAD request, returning the response for error statuses too.

  Args:
    url: The URL to request.
    headers: A dict of request headers.

  Returns:
    Either an http.client.HTTPResponse or a urllib.error.HTTPError; both expose
    'code' and 'headers'.
  """
  request = urllib.request.Request(url, headers=headers, method='HEAD')
  try:
    return urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS)
  except urllib.error.HTTPError as e:
    return e


def _parse_bearer_challenge(header):
  """Parses a WWW-Authenticate Bearer challenge into its parameters.

  Args:
    header: A header value such as 'Bearer realm="https://gcr.io/v2/token",
      service="gcr.io",scope="repository:google.com/x:pull"'.

  Returns:
    A dict of the quoted challenge parameters, or None if the challenge is not
    a Bearer challenge.
  """
  scheme, _, parameters = header.partition(' ')
  if scheme.strip().lower() != 'bearer':
    return None
  return dict(re.findall(r'([a-zA-Z_]+)="([^"]*)"', parameters))


def _fetch_anonymous_token(challenge):
  """Fetches an anonymous pull token from the realm named in a challenge.

  Args:
    challenge: The parsed WWW-Authenticate parameters, which must contain
      'realm'.

  Returns:
    A bearer token string, or None if the realm did not return one.

  Raises:
    urllib.error.HTTPError: If the realm refuses to issue an anonymous token,
      as Artifact Registry does for repositories that are not publicly
      readable.
  """
  realm = challenge.get('realm')
  if not realm:
    return None

  query = {k: challenge[k] for k in ('service', 'scope') if challenge.get(k)}
  url = realm
  if query:
    url = '{}?{}'.format(realm, urllib.parse.urlencode(query))

  with urllib.request.urlopen(url, timeout=_TIMEOUT_SECONDS) as response:
    body = json.loads(response.read().decode('utf-8'))

  # Registries differ on which field carries the token.
  return body.get('token') or body.get('access_token')


class CloudSdkImageTest(unittest.TestCase):

  def testEnvironmentVariableOverridesImage(self):
    """DSUB_CLOUD_SDK_IMAGE lets an operator pin a known-good image."""
    self.addCleanup(importlib.reload, google_utils)

    override = 'gcr.io/google.com/cloudsdktool/cloud-sdk:537.0.0-slim'
    with mock.patch.dict(os.environ, {'DSUB_CLOUD_SDK_IMAGE': override}):
      reloaded = importlib.reload(google_utils)
      self.assertEqual(override, reloaded.CLOUD_SDK_IMAGE)

  def testEmptyEnvironmentVariableUsesDefaultImage(self):
    """An unset or empty override must not produce an empty image name."""
    self.addCleanup(importlib.reload, google_utils)

    with mock.patch.dict(os.environ, {'DSUB_CLOUD_SDK_IMAGE': ''}):
      reloaded = importlib.reload(google_utils)
      self.assertEqual(reloaded.DEFAULT_CLOUD_SDK_IMAGE,
                       reloaded.CLOUD_SDK_IMAGE)

  def testCloudSdkImageExistsInRegistry(self):
    """The image the providers will actually pull must resolve to a manifest."""
    if os.environ.get('DSUB_SKIP_NETWORK_TESTS'):
      self.skipTest('DSUB_SKIP_NETWORK_TESTS is set')

    # Check the image that will actually be pulled, so that an operator
    # who has set DSUB_CLOUD_SDK_IMAGE validates their own pin.
    image = google_utils.CLOUD_SDK_IMAGE
    registry, repository, reference = _split_image(image)
    url = 'https://{}/v2/{}/manifests/{}'.format(registry, repository,
                                                 reference)
    headers = {'Accept': _ACCEPT_MANIFEST_TYPES}

    # _head() returns HTTP error statuses rather than raising, so a URLError
    # from it always means the registry itself was unreachable.
    try:
      response = _head(url, headers)
    except urllib.error.URLError as e:
      self.skipTest('Could not reach {}: {}'.format(registry, e))

    # Public images on gcr.io answer anonymously, but Artifact Registry and
    # Docker Hub demand a token even for public reads. Both advertise where to
    # get one, so honor the challenge rather than hard-coding a registry. Note
    # that a 404 never reaches this branch, so a missing image is still a
    # failure below.
    if response.code in (401, 403):
      challenge = _parse_bearer_challenge(
          response.headers.get('WWW-Authenticate', ''))
      if not challenge:
        self.skipTest('{} requires credentials to read {} and did not offer a '
                      'bearer challenge (HTTP {}).'.format(
                          registry, repository, response.code))

      try:
        token = _fetch_anonymous_token(challenge)
      except urllib.error.HTTPError as e:
        # Artifact Registry, for one, refuses anonymous pull tokens. An
        # anonymous check cannot tell "deleted" from "private" in that case, so
        # skip rather than claim a breakage that may not exist.
        self.skipTest('{} refused an anonymous pull token for {} (HTTP {}). '
                      'Cannot verify this image without credentials.'.format(
                          registry, repository, e.code))
      except urllib.error.URLError as e:
        self.skipTest('Could not reach the token realm for {}: {}'.format(
            registry, e))

      if not token:
        self.skipTest(
            '{} did not return an anonymous pull token for {}.'.format(
                registry, repository))

      headers['Authorization'] = 'Bearer {}'.format(token)
      try:
        response = _head(url, headers)
      except urllib.error.URLError as e:
        self.skipTest('Could not reach {}: {}'.format(registry, e))

    self.assertEqual(
        200, response.code,
        'CLOUD_SDK_IMAGE {!r} did not resolve in the registry (HTTP {}). '
        'The Google providers run localization, delocalization, and logging '
        'in this image, so jobs will fail at runtime until it is fixed. Check '
        'whether the tag was deleted by the gcr.io retention policy, or '
        'whether the repository was renamed. See '
        'dsub/providers/google_utils.py.'.format(image, response.code))


if __name__ == '__main__':
  unittest.main()
