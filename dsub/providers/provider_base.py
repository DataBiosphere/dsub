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

"""Interface for job providers."""

from . import google
from . import local
from . import test_fails


def get_provider(args):
  """Returns a provider for job submission requests."""

  provider = getattr(args, 'provider', 'google')

  if provider == 'google':
    return google.GoogleJobProvider(
        getattr(args, 'verbose', False),
        getattr(args, 'dry_run', False), args.project)
  elif provider == 'local':
    return local.LocalJobProvider()
  elif provider == 'test-fails':
    return test_fails.FailsJobProvider()
  else:
    raise ValueError('Unknown provider: ' + provider)


def add_provider_argument(parser):
  parser.add_argument(
      '--provider',
      default='google',
      choices=['local', 'google', 'test-fails'],
      help="""Job service provider. Valid values are "google" (Google's
        Pipeline API) and "local" (local Docker execution). "test-*" providers
        are for testing purposes only.""",
      metavar='PROVIDER')


def get_dstat_provider_args(args):
  """A string with the arguments to point dstat to the same provider+project."""
  if args.provider == 'google':
    return ' --project %s' % args.project
  elif args.provider == 'local':
    return ' --provider local'
  elif args.provider == 'test-fails':
    return ''
  # New providers should add their dstat required arguments here.
  assert False
  return ''


def get_ddel_provider_args(args):
  """A string with the arguments to point ddel to the same provider+project."""
  # Change this if the two ever diverge.
  return get_dstat_provider_args(args)


def check_for_unsupported_flag(args):
  """Raise an error if the provider doesn't support a provided flag."""
  if args.label and args.provider not in ['test-fails', 'local']:
    raise ValueError(
        '--label is not supported by the "%s" provider.' % args.provider)


if __name__ == '__main__':
  pass
