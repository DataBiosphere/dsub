# Lint as: python2, python3
# Copyright 2017 Google Inc. All Rights Reserved.
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
"""Interface for the providers and tasks.

The job submission model for dsub assumes that each call to dsub is for a single
job. Each job may contain tasks. Tasks are homogeneous in that they run the same
code and differ only in their input and output values.

This model allows one to submit (dsub), monitor (dstat), and delete (ddel) a set
of tasks as a single unit.

To submit a job with a list of tasks, use the dsub "--tasks" parameter. dsub
will assign task ids to each task.

Nomenclature for objects passed between dsub and the provider can be a bit
subtle. In an attempt toward consistency of nomenclature, a job which does not
have explicit "tasks" will have a single implicit task, but no "task id". Thus
nomenclature below will indicate "task data" even for such jobs that do not have
explicit tasks.
"""

import abc
import six


@six.add_metaclass(abc.ABCMeta)
class JobProvider(object):
  """Interface all job providers should inherit from."""

  # A member field that a provider can use to expose a status message
  # such as a deprecation notice.
  status_message = None

  @abc.abstractmethod
  def prepare_job_metadata(self, script, job_name, user_id):
    """Returns a dictionary of metadata fields for the job.

    Call this before calling submit_job.

    The job metadata is a dictionary of values relevant to the job as a whole,
    such as:

    * job-name
    * job-id
    * user-id
    * script
    * dsub-version

    The creation of the job metadata is done by the provider, as the rules
    around these items are provider-specific. For example, one job provider
    might have an 8 character restriction on job names, while another may be
    restricted to lower-case characters.

    The provider *must* set the job-name, job-id, and user-id. They may be used
    by the dsub infrastructure for display and direct return to callers.

    The provider is free to set any other key that it will need in
    submit_job().

    The "job_name" passed in need not be the same 'job-name' value set in the
    returned job metadata, as provider-specific character restrictions may need
    to be applied.

    Args:
      script: path to the job script
      job_name: user-supplied job name, if any
      user_id: user whose jobs to look for
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def submit_job(self, job_descriptor, skip_if_output_present):
    """Submit the job to be executed.

    Args:
      job_descriptor (job_model.JobDescriptor): parameters needed to launch all
      job tasks
      skip_if_output_present: (boolean) if true, skip tasks whose output
        is present (see --skip flag for more explanation).


    Returns:
      A dictionary containing the 'user-id', 'job-id', and 'task-id' list.
      For jobs that are not task array jobs, the task-id list should be empty.
      If all tasks were skipped, then the job-id is dsub_lib.NO_JOB.


    Raises:
      ValueError: submit_job may validate any of the parameters and raise
        a value error if any parameter (or specific combination of parameters)
        is not supported by the provider.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def delete_jobs(self,
                  user_ids,
                  job_ids,
                  task_ids,
                  labels,
                  create_time_min=None,
                  create_time_max=None):
    """Kills the operations associated with the specified job or job.task.

    Some providers may provide only a "cancel" operation, which terminates the
    task but does not truly "delete" it from the "task list".

    Args:
      user_ids: a set of user ids who "own" the job(s) to delete.
      job_ids: a set of job ids to delete.
      task_ids: a set of task ids to delete.
      labels: a set of LabelParam, each must match the job(s) to be cancelled.
      create_time_min: a timezone-aware datetime value for the earliest create
                       time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
                       create time of a task, inclusive.

    Returns:
      (list of tasks canceled,
       for each task that couldn't be canceled, the error message).

      Only tasks that were running are included in the return value.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def lookup_job_tasks(self,
                       statuses,
                       user_ids=None,
                       job_ids=None,
                       job_names=None,
                       task_ids=None,
                       task_attempts=None,
                       labels=None,
                       create_time_min=None,
                       create_time_max=None,
                       max_tasks=0,
                       page_size=0,
                       verbose=True):
    """Return a list of tasks based on the search criteria.

    If any of the filters are empty or {'*'}, then no filtering is performed on
    that field. Filtering by both a job id list and job name list is
    unsupported.

    Args:
      statuses: {'*'}, or a set of job status strings to return. Valid
        status strings are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_ids: a set of ids for the user(s) who launched the job.
      job_ids: a set of job ids to return.
      job_names: a set of job names to return.
      task_ids: a set of specific tasks within the specified job(s) to return.
      task_attempts: a set of specific task attempts within the specified
        tasks(s) to return.
      labels: a list of LabelParam, each must match the job(s) returned.
      create_time_min: a timezone-aware datetime value for the earliest create
                       time of a task, inclusive.
      create_time_max: a timezone-aware datetime value for the most recent
                       create time of a task, inclusive.
      max_tasks: the maximum number of job tasks to return or 0 for no limit.
      page_size: the page size to use for each query to the backend. May be
                 ignored by provider implementations.
      verbose: if set to true, will output retrying error messages

    Returns:
      A list of Task objects.

    Raises:
      ValueError: if both a job id list and a job name list are provided
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def get_tasks_completion_messages(self, tasks):
    """List of the error message of each given task."""
    raise NotImplementedError()


class Task(object):
  """Basic container for task metadata."""

  @abc.abstractmethod
  def raw_task_data(self):
    """Return a provider-specific representation of task data.

    Returns:
      dictionary of task data from the provider.
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def get_field(self, field, default=None):
    """Return a metadata-field for the task.

    Not all fields need to be supported by all providers.
    Field identifiers include:

    'job-name', 'job-id', 'task-id', 'task-attempt', 'user-id', 'task-status',
    'error-message', 'create-time', 'start-time', 'end-time', 'inputs',
    'outputs', 'events'

    The following are required by dstat:
    - status: The task status ('RUNNING', 'CANCELED', 'FAILED', 'SUCCESS')
    - status-message: A short message that is displayed in the default
                      dstat output. This should be as concise and useful as
                      possible ("Pending", "Running", "Error: invalid...")
    - status-detail: A longer status message that is displayed in full dstat
                     output. Ideally, this is the last few lines of a log which
                     gives the user enough information that they do not need
                     to go to the log files.

    dstat's short output shows status-message.
    dstat's full output shows status and status-detail

    Args:
      field: one of the choices listed above.
      default: the value to return if no value if found.
    """
    raise NotImplementedError()
