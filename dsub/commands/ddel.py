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

"""Delete dsub jobs and tasks.

Follows the model of qdel.
"""
from __future__ import print_function

import sys

from ..lib import dsub_util
from ..lib import job_model
from ..lib import param_util
from ..lib import resources
from ..providers import provider_base


def _parse_arguments():
  """Parses command line arguments.

  Returns:
    A Namespace of parsed arguments.
  """
  # Handle version flag and exit if it was passed.
  param_util.handle_version_flag()

  parser = provider_base.create_parser(sys.argv[0])

  parser.add_argument(
      '--version', '-v', default=False, help='Print the dsub version and exit.')

  parser.add_argument(
      '--jobs',
      '-j',
      required=True,
      nargs='*',
      help='List of job-ids to delete. Use "*" to delete all running jobs.')
  parser.add_argument(
      '--tasks',
      '-t',
      nargs='*',
      help='List of tasks in an array job to delete.')
  parser.add_argument(
      '--users',
      '-u',
      nargs='*',
      default=[],
      help="""Deletes only those jobs which were submitted by the list of users.
          Use "*" to delete jobs of any user.""")
  parser.add_argument(
      '--age',
      help="""Deletes only those jobs newer than the specified age. Ages can be
          listed using a number followed by a unit. Supported units are
          s (seconds), m (minutes), h (hours), d (days), w (weeks).
          For example: '7d' (7 days). Bare numbers are treated as UTC.""")
  parser.add_argument(
      '--label',
      nargs='*',
      action=param_util.ListParamAction,
      default=[],
      help='User labels to match. Tasks returned must match all labels.',
      metavar='KEY=VALUE')

  # Shared between the "google-cls-v2" and "google-v2" providers
  google_common = parser.add_argument_group(
      title='google-common',
      description="""Options common to the "google", "google-cls-v2", and
        "google-v2" providers""")
  google_common.add_argument(
      '--project',
      help='Cloud project ID in which to find and delete the job(s)')

  google_cls_v2 = parser.add_argument_group(
      title='"google-cls-v2" provider options',
      description='See also the "google-common" options listed')
  google_cls_v2.add_argument(
      '--location',
      default=job_model.DEFAULT_LOCATION,
      help="""Specifies the Google Cloud region to which the dsub job was
        submitted. (default: {})""".format(job_model.DEFAULT_LOCATION))

  return provider_base.parse_args(
      parser, {
          'google-cls-v2': ['project'],
          'google-v2': ['project'],
          'test-fails': [],
          'local': [],
      }, sys.argv[1:])


def _emit_search_criteria(user_ids, job_ids, task_ids, labels):
  """Print the filters used to delete tasks. Use raw flags as arguments."""
  print('Delete running jobs:')
  print('  user:')
  print('    %s\n' % user_ids)
  print('  job-id:')
  print('    %s\n' % job_ids)
  if task_ids:
    print('  task-id:')
    print('    %s\n' % task_ids)
  # Labels are in a LabelParam namedtuple and must be reformated for printing.
  if labels:
    print('  labels:')
    print('    %s\n' % repr(labels))


def main():
  # Parse args and validate
  args = _parse_arguments()

  # Compute the age filter (if any)
  create_time = param_util.age_to_create_time(args.age)

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args, resources)

  # Make sure users were provided, or try to fill from OS user. This cannot
  # be made into a default argument since some environments lack the ability
  # to provide a username automatically.
  user_ids = set(args.users) if args.users else {dsub_util.get_os_user()}

  # Process user labels.
  labels = param_util.parse_pair_args(args.label, job_model.LabelParam)

  # Let the user know which jobs we are going to look up
  with dsub_util.replace_print():
    provider_base.emit_provider_message(provider)

    _emit_search_criteria(user_ids, args.jobs, args.tasks, args.label)
    # Delete the requested jobs
    deleted_tasks = ddel_tasks(
        provider,
        user_ids=user_ids,
        job_ids=set(args.jobs) if args.jobs else None,
        task_ids=set(args.tasks) if args.tasks else None,
        labels=labels,
        create_time_min=create_time)
    # Emit the count of deleted jobs.
    # Only emit anything about tasks if any of the jobs contains a task-id.
    deleted_jobs = dsub_util.tasks_to_job_ids(deleted_tasks)
    job_count = len(deleted_jobs)

    deleted_tasks = [
        t for t in deleted_tasks if t.get_field('task-id')
    ]

    tasks_msg = ''
    if deleted_tasks:
      task_count = len(deleted_tasks)
      tasks_msg = ' (%d task%s)' % (task_count, '' if task_count == 1 else 's')

  print('%d job%s deleted%s' % (job_count, ''
                                if job_count == 1 else 's', tasks_msg))


def ddel_tasks(provider,
               user_ids=None,
               job_ids=None,
               task_ids=None,
               labels=None,
               create_time_min=None,
               create_time_max=None):
  """Kill jobs or job tasks.

  This function separates ddel logic from flag parsing and user output. Users
  of ddel who intend to access the data programmatically should use this.

  Args:
    provider: an instantiated dsub provider.
    user_ids: a set of user ids who "own" the job(s) to delete.
    job_ids: a set of job ids to delete.
    task_ids: a set of task ids to delete.
    labels: a set of LabelParam, each must match the job(s) to be cancelled.
    create_time_min: a timezone-aware datetime value for the earliest create
                     time of a task, inclusive.
    create_time_max: a timezone-aware datetime value for the most recent create
                     time of a task, inclusive.

  Returns:
    list of job ids which were deleted.
  """
  # Delete the requested jobs
  deleted_tasks, error_messages = provider.delete_jobs(
      user_ids, job_ids, task_ids, labels, create_time_min, create_time_max)

  # Emit any errors canceling jobs
  for msg in error_messages:
    print(msg)

  return deleted_tasks


if __name__ == '__main__':
  main()
