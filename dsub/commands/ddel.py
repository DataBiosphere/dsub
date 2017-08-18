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
import argparse

from ..lib import dsub_util
from ..lib import param_util
from ..providers import provider_base


def parse_arguments():
  """Parses command line arguments.

  Returns:
    A Namespace of parsed arguments.
  """
  provider_required_args = {
      'google': ['project'],
      'test-fails': [],
      'local': [],
  }
  epilog = 'Provider-required arguments:\n'
  for provider in provider_required_args:
    epilog += '  %s: %s\n' % (provider, provider_required_args[provider])
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter, epilog=epilog)
  provider_base.add_provider_argument(parser)
  google = parser.add_argument_group(
      title='google',
      description='Options for the Google provider (Pipelines API)')
  google.add_argument(
      '--project',
      help='Cloud project ID in which to find and delete the job(s)')
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
  return parser.parse_args()


def emit_search_criteria(users, jobs, tasks):
  print 'Delete running jobs:'
  print '  user:'
  print '    %s\n' % users
  print '  job-id:'
  print '    %s\n' % jobs
  if tasks:
    print '  task-id:'
    print '    %s\n' % tasks


def main():
  # Parse args and validate
  args = parse_arguments()

  # Compute the age filter (if any)
  create_time = param_util.age_to_create_time(args.age)

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Make sure users were provided, or try to fill from OS user. This cannot
  # be made into a default argument since some environments lack the ability
  # to provide a username automatically.
  user_list = args.users if args.users else [dsub_util.get_os_user()]

  # Let the user know which jobs we are going to look up
  with dsub_util.replace_print():
    emit_search_criteria(user_list, args.jobs, args.tasks)

    # Delete the requested jobs
    deleted_tasks, error_messages = provider.delete_jobs(
        user_list, args.jobs, args.tasks, create_time)

    # Emit any errors canceling jobs
    for msg in error_messages:
      print msg

    # Emit the count of deleted jobs.
    # Only emit anything about tasks if any of the jobs contains a task-id.
    deleted_jobs = dsub_util.tasks_to_job_ids(provider, deleted_tasks)
    job_count = len(deleted_jobs)

    deleted_tasks = [
        t for t in deleted_tasks if provider.get_task_field(t, 'task-id')
    ]

    tasks_msg = ''
    if deleted_tasks:
      task_count = len(deleted_tasks)
      tasks_msg = ' (%d task%s)' % (task_count, '' if task_count == 1 else 's')

  print '%d job%s deleted%s' % (job_count, ''
                                if job_count == 1 else 's', tasks_msg)


if __name__ == '__main__':
  main()
