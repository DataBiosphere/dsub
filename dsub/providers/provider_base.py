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
import argparse

from . import google
from . import local
from . import test_fails


PROVIDER_NAME_MAP = {
    google.GoogleJobProvider: 'google',
    local.LocalJobProvider: 'local',
    test_fails.FailsJobProvider: 'test-fails',
}


def get_provider(args, resources):
  """Returns a provider for job submission requests."""

  provider = getattr(args, 'provider', 'google')

  if provider == 'google':
    return google.GoogleJobProvider(
        getattr(args, 'verbose', False),
        getattr(args, 'dry_run', False), args.project)
  elif provider == 'local':
    return local.LocalJobProvider(resources)
  elif provider == 'test-fails':
    return test_fails.FailsJobProvider()
  else:
    raise ValueError('Unknown provider: ' + provider)


def get_provider_name(provider):
  """Returns the name of a given provider."""
  return PROVIDER_NAME_MAP[provider.__class__]


class DsubHelpFormatter(argparse.ArgumentDefaultsHelpFormatter,
                        argparse.RawDescriptionHelpFormatter):
  """Display defaults in help and display the epilog in its raw format.

  As described in https://bugs.python.org/issue13023, there is not a built-in
  class to provide both display of defaults as well as displaying the epilog
  just as you want it to. The recommended approach is to create a simple
  subclass of both Formatters.
  """
  pass


def create_parser(prog):
  """Create an argument parser, adding in the list of providers."""
  parser = argparse.ArgumentParser(prog=prog, formatter_class=DsubHelpFormatter)

  parser.add_argument(
      '--provider',
      default='google',
      choices=['local', 'google', 'test-fails'],
      help="""Job service provider. Valid values are "google" (Google's
        Pipeline API) and "local" (local Docker execution). "test-*" providers
        are for testing purposes only.""",
      metavar='PROVIDER')

  return parser


def parse_args(parser, provider_required_args, argv):
  """Add provider required arguments epilog message, parse, and validate."""

  # Add the provider required arguments epilog message
  epilog = 'Provider-required arguments:\n'
  for provider in provider_required_args:
    epilog += '  %s: %s\n' % (provider, provider_required_args[provider])
  parser.epilog = epilog

  # Parse arguments
  args = parser.parse_args(argv)

  # For the selected provider, check the required arguments
  for arg in provider_required_args[args.provider]:
    if not args.__getattribute__(arg):
      parser.error('argument --%s is required' % arg)

  return args


def get_dstat_provider_args(provider, project):
  """A string with the arguments to point dstat to the same provider+project."""
  provider_name = get_provider_name(provider)
  if provider_name == 'google':
    return ' --project %s' % project
  elif provider_name == 'local':
    return ' --provider local'
  elif provider_name == 'test-fails':
    return ''
  # New providers should add their dstat required arguments here.
  assert False
  return ''


def get_ddel_provider_args(provider_type, project):
  """A string with the arguments to point ddel to the same provider+project."""
  # Change this if the two ever diverge.
  return get_dstat_provider_args(provider_type, project)


def check_for_unsupported_flag(args):
  """Raise an error if the provider doesn't support a provided flag."""
  if args.label and args.provider not in ['test-fails', 'local', 'google']:
    raise ValueError(
        '--label is not supported by the "%s" provider.' % args.provider)


if __name__ == '__main__':
  pass
