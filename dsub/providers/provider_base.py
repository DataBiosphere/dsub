# Lint as: python2, python3
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

from __future__ import print_function

import argparse
import os

from . import google_base
from . import google_cls_v2
from . import google_v2
from . import local
from . import test_fails


PROVIDER_NAME_MAP = {
    google_v2.GoogleV2JobProvider: 'google-v2',
    google_cls_v2.GoogleCLSV2JobProvider: 'google-cls-v2',
    local.LocalJobProvider: 'local',
    test_fails.FailsJobProvider: 'test-fails',
}


def credentials_from_args(args):
  credentials = getattr(args, 'credentials', None)
  credentials_file = getattr(args, 'credentials_file', None)
  if credentials_file and not credentials:
    credentials = google_base.credentials_from_service_account_info(
        credentials_file)
  return credentials


def get_provider(args, resources):
  """Returns a provider for job submission requests."""

  provider = getattr(args, 'provider', 'google-v2')

  if provider == 'google-cls-v2':
    return google_cls_v2.GoogleCLSV2JobProvider(
        getattr(args, 'dry_run', False),
        args.project,
        args.location,
        credentials=credentials_from_args(args))
  elif provider == 'google-v2':
    return google_v2.GoogleV2JobProvider(
        getattr(args, 'dry_run', False),
        args.project,
        credentials=credentials_from_args(args))
  elif provider == 'local':
    return local.LocalJobProvider(resources)
  elif provider == 'test-fails':
    return test_fails.FailsJobProvider()
  else:
    raise ValueError('Unknown provider: ' + provider)


def get_provider_name(provider):
  """Returns the name of a given provider."""
  return PROVIDER_NAME_MAP[provider.__class__]


def create_parser(prog):
  """Create an argument parser, adding in the list of providers."""
  parser = argparse.ArgumentParser(
      prog=prog, formatter_class=argparse.RawDescriptionHelpFormatter)

  parser.add_argument(
      '--provider',
      default='google-v2',
      choices=['local', 'google-v2', 'google-cls-v2', 'test-fails'],
      help="""Job service provider. Valid values are "google-v2" (Google's
        Pipeline API v2alpha1), "google-cls-v2" (Google's Pipelines API v2beta)
        and "local" (local Docker execution).
        "test-*" providers are for testing purposes only.
        (default: google-v2)""",
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


def get_dstat_provider_args(provider, project, location):
  """A string with the arguments to point dstat to the same provider+project."""
  provider_name = get_provider_name(provider)

  args = []
  if provider_name == 'google-cls-v2':
    args.append('--project %s --location %s' % (project, location))
  elif provider_name == 'google-v2':
    args.append('--project %s' % project)
  elif provider_name == 'local':
    pass
  elif provider_name == 'test-fails':
    pass
  else:
    # New providers should add their dstat required arguments here.
    assert False, 'Provider %s needs get_dstat_provider_args support' % provider

  args.insert(0, '--provider %s' % provider_name)
  return ' '.join(args)


def get_ddel_provider_args(provider_type, project, location):
  """A string with the arguments to point ddel to the same provider+project."""
  # Change this if the two ever diverge.
  return get_dstat_provider_args(provider_type, project, location)


def emit_provider_message(provider):
  if provider.status_message:
    print(provider.status_message)


def check_for_unsupported_flag(args):
  """Raise an error if the provider doesn't support a provided flag."""
  if args.label and args.provider not in [
      'test-fails', 'local', 'google-v2', 'google-cls-v2'
  ]:
    raise ValueError(
        '--label is not supported by the "%s" provider.' % args.provider)


def _format_task_uri(fmt, job_metadata, task_metadata):
  """Returns a URI with placeholders replaced by metadata values."""

  values = {
      'job-id': None,
      'task-id': 'task',
      'job-name': None,
      'user-id': None,
      'task-attempt': None
  }
  for key in values:
    values[key] = task_metadata.get(key) or job_metadata.get(key) or values[key]

  return fmt.format(**values)


def format_logging_uri(uri, job_metadata, task_metadata):
  """Inserts task metadata into the logging URI.

  The core behavior is inspired by the Google Pipelines API:
    (1) If a the uri ends in ".log", then that is the logging path.
    (2) Otherwise, the uri is treated as "directory" for logs and a filename
        needs to be automatically generated.

  For (1), if the job is a --tasks job, then the {task-id} is inserted
  before ".log".

  For (2), the file name generated is {job-id}, or for --tasks jobs, it is
  {job-id}.{task-id}.

  In both cases .{task-attempt} is inserted before .log for --retries jobs.

  In addition, full task metadata substitution is supported. The URI
  may include substitution strings such as
  "{job-id}", "{task-id}", "{job-name}", "{user-id}", and "{task-attempt}".

  Args:
    uri: User-specified logging URI which may contain substitution fields.
    job_metadata: job-global metadata.
    task_metadata: tasks-specific metadata.

  Returns:
    The logging_uri formatted as described above.
  """

  # If the user specifies any formatting (with curly braces), then use that
  # as the format string unchanged.
  fmt = str(uri)
  if '{' not in fmt:
    if uri.endswith('.log'):
      # URI includes a filename. Trim the extension and just use the prefix.
      fmt = os.path.splitext(uri)[0]
    else:
      # URI is a path to a directory. The job-id becomes the filename prefix.
      fmt = os.path.join(uri, '{job-id}')

    # If this is a task job, add the task-id.
    if task_metadata.get('task-id') is not None:
      fmt += '.{task-id}'

    # If this is a retryable task, add the task-attempt.
    if task_metadata.get('task-attempt') is not None:
      fmt += '.{task-attempt}'

    fmt += '.log'

  return _format_task_uri(fmt, job_metadata, task_metadata)


if __name__ == '__main__':
  pass
