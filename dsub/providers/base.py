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
"""Interface for the providers.

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

from abc import ABCMeta
from abc import abstractmethod


class JobProvider(object):
  """Interface all job providers should inherit from."""
  __metaclass__ = ABCMeta

  @abstractmethod
  def prepare_job_metadata(self, script, job_name, user_id):
    """Returns a dictionary of metadata fields for the job.

    Call this before calling submit_job.

    The job metadata is a dictionary of values relevant to the job as a whole,
    such as:

    * job-name
    * job-id
    * user-id
    * script

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
      job_name: ?
      user_id: user whose jobs to look for
    """
    raise NotImplementedError()

  @abstractmethod
  def submit_job(self, job_resources, job_metadata, all_task_data):
    """Submit the job to be executed.

    Args:
      job_resources: resource parameters required by each job.
      job_metadata: job parameters such as job-id, user-id, script.
      all_task_data: list of parameters to launch each job task.

    job_resources contains settings related to how many resources to give each
    task. Its fields include: min_cores, min_ram, disk_size, boot_disk_size,
    preemptible, image, zones.

    job_metadata is a dictionary. Its fields include: 'script', 'pipeline-name',
    'job-name', 'job-id', 'user-id', 'is_table'.

    task_parameters is a list of dictionaries, one per task to execute.
    Each contains the following fields: 'envs', 'inputs', 'outputs'.

    Returns:
      A dictionary containing the 'user-id', 'job-id', and 'task-id' list.
      For jobs that are not task array jobs, the task-id list should be empty.

    Raises:
      ValueError: submit job may validate any of the parameters and raise
        a value error if any parameter (or specific combination of parameters)
        is not supported by the provider.
    """
    raise NotImplementedError()

  @abstractmethod
  def delete_jobs(self, user_list, job_list, task_list, create_time=None):
    """Kills the operations associated with the specified job or job.task.

    Some providers may provide only a "cancel" operation, which terminates the
    task but does not truly "delete" it from the "task list".

    Args:
      user_list: List of user ids who "own" the job(s) to delete.
      job_list: List of job ids to delete.
      task_list: List of task ids to delete.
      create_time: a UTC value for earliest create time for a task.

    Returns:
      (list of tasks canceled,
       for each task that couldn't be canceled, the error message).

      Only tasks that were running are included in the return value.
    """
    raise NotImplementedError()

  @abstractmethod
  def lookup_job_tasks(self,
                       status_list,
                       user_list=None,
                       job_list=None,
                       job_name_list=None,
                       task_list=None,
                       create_time=None,
                       max_tasks=0):
    """Return a list of tasks based on the search criteria.

    If any of the filters are empty or "[*]", then no filtering is performed on
    that field. Filtering by both a job id list and job name list is
    unsupported.

    Args:
      status_list: ['*'], or a list of job status strings to return. Valid
        status strings are 'RUNNING', 'SUCCESS', 'FAILURE', or 'CANCELED'.
      user_list: a list of ids for the user(s) who launched the job.
      job_list: a list of job ids to return.
      job_name_list: a list of job names to return.
      task_list: a list of specific tasks within the specified job(s) to return.
      create_time: a UTC value for earliest create time for a task.
      max_tasks: the maximum number of job tasks to return or 0 for no limit.

    Returns:
      A list of provider-specific objects, each representing a submitted task.

    Raises:
      ValueError: if both a job id list and a job name list are provided
    """
    raise NotImplementedError()

  @abstractmethod
  def get_task_field(self, task, field):
    """Return a field from the provider-specific task object.

    Not all fields need to be supported by all providers.
    Field identifiers include:

    'job-name', 'job-id', 'task-id', 'user-id',
    'job-status', 'error-message', 'create-time', 'end-time'
    'inputs', 'outputs'

    The following are needed by dstat:
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
      task: object returned by lookup_job_tasks
      field: one of the choices listed above.
    """
    raise NotImplementedError()

  @abstractmethod
  def get_task_status_message(self, task):
    """The 'error-message' from the task, or status if no error."""
    raise NotImplementedError()

  @abstractmethod
  def get_tasks_completion_messages(self, tasks):
    """List of the error message of each given task."""
    raise NotImplementedError()
