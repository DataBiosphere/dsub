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
from ..providers import provider_base


def parse_arguments():
  """Parses command line arguments.

  Returns:
    A Namespace of parsed arguments.
  """
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--project',
      required=True,
      help='Cloud project ID in which to find and delete the job(s)')
  parser.add_argument(
      '-j',
      '--jobs',
      required=True,
      nargs='*',
      help='List of job-ids to delete. Use "*" to delete all running jobs.')
  parser.add_argument(
      '-t',
      '--tasks',
      nargs='*',
      help='List of tasks in an array job to delete.')
  parser.add_argument(
      '-u',
      '--users',
      nargs='*',
      default=[dsub_util.get_default_user()],
      help="""Deletes only those jobs which were submitted by the list of users.
          Use "*" to delete jobs of any user.""")
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

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Let the user know which jobs we are going to look up
  with dsub_util.replace_print():
    emit_search_criteria(args.users, args.jobs, args.tasks)

  # Delete the requested jobs
  deleted_tasks, _ = provider.delete_jobs(args.users, args.jobs, args.tasks)

  # Emit the count of deleted jobs.
  # Only emit anything about tasks if any of the jobs contains a task-id value.
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
