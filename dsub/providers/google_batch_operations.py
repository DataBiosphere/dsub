# Copyright 2022 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility routines for constructing a Google Batch API request."""
from typing import List, Optional, Dict

# pylint: disable=g-import-not-at-top
try:
  from google.cloud import batch_v1
except ImportError:
  # TODO: Remove conditional import when batch library is available
  from . import batch_dummy as batch_v1
# pylint: enable=g-import-not-at-top


def label_filter(label_key: str, label_value: str) -> str:
  """Return a valid label filter for operations.list()."""
  return 'labels."{}" = "{}"'.format(label_key, label_value)


def get_label(op: batch_v1.types.Job, name: str) -> str:
  """Return the value for the specified label."""
  return op.labels.get(name)


def get_environment(op: batch_v1.types.Job) -> Dict[str, str]:
  # Currently Batch only supports task_groups of size 1
  task_group = op.task_groups[0]
  env_dict = {}
  for env in task_group.task_environments:
    env_dict.update(env.variables)
  return env_dict


def is_done(op: batch_v1.types.Job) -> bool:
  """Return whether the operation has been marked done."""
  return op.status.state in [
      batch_v1.types.job.JobStatus.State.SUCCEEDED,
      batch_v1.types.job.JobStatus.State.FAILED
  ]


def is_success(op: batch_v1.types.Job) -> bool:
  """Return whether the operation has completed successfully."""
  return op.status.state == batch_v1.types.job.JobStatus.State.SUCCEEDED


def is_canceled() -> bool:
  """Return whether the operation was canceled by the user."""
  # TODO: Verify if the batch job has a canceled enum
  return False


def is_failed(op: batch_v1.types.Job) -> bool:
  """Return whether the operation has failed."""
  return op.status.state == batch_v1.types.job.JobStatus.State.FAILED


def _pad_timestamps(ts: str) -> str:
  """Batch API removes trailing zeroes from the fractional part of seconds."""
  # ts looks like 2022-06-23T19:38:23.11506605Z
  # Pad zeroes until the fractional part is 9 digits long
  dt, fraction = ts.split('.')
  fraction = fraction.rstrip('Z')
  fraction = fraction.ljust(9, '0')
  return f'{dt}.{fraction}Z'


def get_update_time(op: batch_v1.types.Job) -> Optional[str]:
  """Return the update time string of the operation."""
  update_time = op.update_time
  if update_time:
    return _pad_timestamps(op.update_time.rfc3339())
  else:
    return None


def get_create_time(op: batch_v1.types.Job) -> Optional[str]:
  """Return the create time string of the operation."""
  create_time = op.create_time
  if create_time:
    return _pad_timestamps(op.create_time.rfc3339())
  else:
    return None


def get_status_events(op: batch_v1.types.Job):
  return op.status.status_events


def build_job(task_groups: List[batch_v1.types.TaskGroup],
              allocation_policy: batch_v1.types.AllocationPolicy,
              labels: Dict[str, str],
              logs_policy: batch_v1.types.LogsPolicy) -> batch_v1.types.Job:
  job = batch_v1.Job()
  job.task_groups = task_groups
  job.allocation_policy = allocation_policy
  job.labels = labels
  job.logs_policy = logs_policy
  return job


def build_task_spec(
    runnables: List[batch_v1.types.task.Runnable],
    volumes: List[batch_v1.types.Volume]) -> batch_v1.types.TaskSpec:
  task_spec = batch_v1.TaskSpec()
  task_spec.runnables = runnables
  task_spec.volumes = volumes
  return task_spec


def build_environment(env_vars: Dict[str, str]):
  environment = batch_v1.Environment()
  environment.variables = env_vars
  return environment


def build_task_group(task_spec: batch_v1.types.TaskSpec,
                     task_environments: List[batch_v1.types.Environment],
                     task_count: int,
                     task_count_per_node: int) -> batch_v1.types.TaskGroup:
  """Build a TaskGroup object for a Batch request.

  Args:
    task_spec (TaskSpec): TaskSpec object
    task_environments (List[Environment]): List of Environment objects
    task_count (int): The number of total tasks in the job
    task_count_per_node (int): The number of tasks to schedule on one VM

  Returns:
    A TaskGroup object.
  """
  task_group = batch_v1.TaskGroup()
  task_group.task_spec = task_spec
  task_group.task_environments = task_environments
  task_group.task_count = task_count
  task_group.task_count_per_node = task_count_per_node
  return task_group


def build_container(
    image_uri: str, entrypoint: str, volumes: List[str],
    commands: List[str]) -> batch_v1.types.task.Runnable.Container:
  container = batch_v1.types.task.Runnable.Container()
  container.image_uri = image_uri
  container.entrypoint = entrypoint
  container.commands = commands
  container.volumes = volumes
  return container


def build_runnable(image_uri: str, entrypoint: str, commands: List[str],
                   run_in_background: bool, volumes: List[str],
                   always_run: bool) -> batch_v1.types.task.Runnable:
  container = build_container(image_uri, entrypoint, volumes, commands)
  runnable = batch_v1.Runnable()
  runnable.container = container
  runnable.background = run_in_background
  runnable.always_run = always_run
  return runnable


def build_volume(disk: str, path: str) -> batch_v1.types.Volume:
  """Build a Volume object for a Batch request.

  Args:
    disk (str): Name of disk to mount, as specified in the resources section.
    path (str): Path to mount the disk at inside the container.

  Returns:
    An object representing a Mount.
  """
  volume = batch_v1.Volume()
  volume.device_name = disk
  volume.mount_path = path
  return volume


def build_allocation_policy(
    ipts: List[batch_v1.types.AllocationPolicy.InstancePolicyOrTemplate]
) -> batch_v1.types.AllocationPolicy:
  allocation_policy = batch_v1.AllocationPolicy()
  allocation_policy.instances = ipts
  return allocation_policy


def build_instance_policy_or_template(
    instance_policy: batch_v1.types.AllocationPolicy.InstancePolicy
) -> batch_v1.types.AllocationPolicy.InstancePolicyOrTemplate:
  ipt = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
  ipt.policy = instance_policy
  return ipt


def build_instance_policy(
    disks: List[batch_v1.types.AllocationPolicy.AttachedDisk]
) -> batch_v1.types.AllocationPolicy.InstancePolicy:
  instance_policy = batch_v1.AllocationPolicy.InstancePolicy()
  instance_policy.disks = [disks]
  return instance_policy


def build_attached_disk(
    disk: batch_v1.types.AllocationPolicy.Disk,
    device_name: str) -> batch_v1.types.AllocationPolicy.AttachedDisk:
  attached_disk = batch_v1.AllocationPolicy.AttachedDisk()
  attached_disk.new_disk = disk
  attached_disk.device_name = device_name
  return attached_disk


def build_persistent_disk(
    size_gb: int, disk_type: str) -> batch_v1.types.AllocationPolicy.Disk:
  disk = batch_v1.AllocationPolicy.Disk()
  disk.type = disk_type
  disk.size_gb = size_gb
  return disk
