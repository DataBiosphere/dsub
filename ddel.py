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

from lib import dsub_util
from providers import provider_base


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
      '--job-list',
      required=True,
      nargs='*',
      help='List of job-ids to delete')
  parser.add_argument(
      '-t',
      '--task-list',
      nargs='*',
      help='List of tasks in an array job to delete.')
  parser.add_argument(
      '-u',
      '--user-list',
      default=[dsub_util.get_default_user()],
      help="""Deletes only those jobs which were submitted by the list of users.
          Use "*" to delete jobs of any user.""")
  return parser.parse_args()


def main():
  # Parse args and validate
  args = parse_arguments()

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Delete the requested jobs
  deleted_jobs, error_messages = provider.delete_jobs(args.user_list,
                                                      args.job_list,
                                                      args.task_list)
  count = len(deleted_jobs)

  print '%d job%s deleted' % (count, '' if count == 1 else 's')
  for msg in error_messages:
    print msg


if __name__ == '__main__':
  main()
