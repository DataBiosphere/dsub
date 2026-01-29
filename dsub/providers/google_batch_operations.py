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
from typing import List, Optional, Dict, MutableSequence

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


def get_environment(
    op: batch_v1.types.Job, runnable_index: int
) -> Dict[str, str]:
  # Currently Batch only supports task_groups of size 1
  task_group = op.task_groups[0]
  task_spec = task_group.task_spec
  runnables = task_spec.runnables
  return runnables[runnable_index].environment.variables


def is_done(op: batch_v1.types.Job) -> bool:
  """Return whether the operation has been marked done."""
  return op.status.state in [
      batch_v1.types.job.JobStatus.State.SUCCEEDED,
      batch_v1.types.job.JobStatus.State.FAILED,
      batch_v1.types.job.JobStatus.State.CANCELLED,
  ]


def is_success(op: batch_v1.types.Job) -> bool:
  """Return whether the operation has completed successfully."""
  return op.status.state == batch_v1.types.job.JobStatus.State.SUCCEEDED


def is_canceled(op: batch_v1.types.Job) -> bool:
  """Return whether the operation was canceled by the user."""
  return op.status.state == batch_v1.types.job.JobStatus.State.CANCELLED


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


def get_preemptible(op: batch_v1.types.Job) -> bool:
  pm = op.allocation_policy.instances[0].policy.provisioning_model
  if pm == batch_v1.AllocationPolicy.ProvisioningModel.SPOT:
    return True
  elif pm == batch_v1.AllocationPolicy.ProvisioningModel.STANDARD:
    return False
  else:
    raise ValueError(f'Invalid provisioning_model value: {pm}')


def get_boot_disk_size(op: batch_v1.types.Job) -> int:
  return op.allocation_policy.instances[0].policy.boot_disk.size_gb


def get_disk_size(op: batch_v1.types.Job) -> int:
  return op.allocation_policy.instances[0].policy.disks[0].new_disk.size_gb


def get_disk_type(op: batch_v1.types.Job) -> str:
  return op.allocation_policy.instances[0].policy.disks[0].new_disk.type


def get_machine_type(op: batch_v1.types.Job) -> str:
  return op.allocation_policy.instances[0].policy.machine_type


def get_zones(op: batch_v1.types.Job) -> List[str]:
  list_of_locations = list(op.allocation_policy.location.allowed_locations)
  # Filter to get only zones and remove the prefix
  zones = [
      location.replace('zones/', '')
      for location in list_of_locations
      if location.startswith('zones/')
  ]
  return zones


def get_regions(op: batch_v1.types.Job) -> List[str]:
  list_of_locations = list(op.allocation_policy.location.allowed_locations)
  # Filter to get only regions and remove the prefix
  regions = [
      location.replace('regions/', '')
      for location in list_of_locations
      if location.startswith('regions/')
  ]
  return regions


def build_job(
    task_groups: List[batch_v1.types.TaskGroup],
    allocation_policy: batch_v1.types.AllocationPolicy,
    labels: Dict[str, str],
    logs_policy: batch_v1.types.LogsPolicy,
) -> batch_v1.types.Job:
  job = batch_v1.Job()
  job.task_groups = task_groups
  job.allocation_policy = allocation_policy
  job.labels = labels
  job.logs_policy = logs_policy
  return job


def build_task_spec(
    runnables: List[batch_v1.types.task.Runnable],
    volumes: List[batch_v1.types.Volume],
    max_run_duration: str,
) -> batch_v1.types.TaskSpec:
  """Build a TaskSpec object for a Batch request.

  Args:
      runnables (List[Runnable]): List of Runnable objects
      volumes (List[Volume]): List of Volume objects

  Returns:
      A TaskSpec object.
  """
  task_spec = batch_v1.TaskSpec()
  task_spec.runnables = runnables
  task_spec.volumes = volumes
  task_spec.max_run_duration = max_run_duration
  return task_spec


def build_environment(env_vars: Dict[str, str]):
  environment = batch_v1.Environment()
  environment.variables = env_vars
  return environment


def build_task_group(
    task_spec: batch_v1.types.TaskSpec,
    task_count: int,
    task_count_per_node: int,
) -> batch_v1.types.TaskGroup:
  """Build a TaskGroup object for a Batch request.

  Args:
    task_spec (TaskSpec): TaskSpec object
    task_count (int): The number of total tasks in the job
    task_count_per_node (int): The number of tasks to schedule on one VM

  Returns:
    A TaskGroup object.
  """
  task_group = batch_v1.TaskGroup()
  task_group.task_spec = task_spec
  task_group.task_count = task_count
  task_group.task_count_per_node = task_count_per_node
  return task_group


def build_container(
    image_uri: str, entrypoint: str, volumes: List[str], commands: List[str], options: Optional[str]
) -> batch_v1.types.task.Runnable.Container:
  container = batch_v1.types.task.Runnable.Container()
  container.image_uri = image_uri
  container.entrypoint = entrypoint
  container.commands = commands
  container.volumes = volumes
  container.options = options
  return container


def build_runnable(
    run_in_background: bool,
    always_run: bool,
    environment: batch_v1.types.Environment,
    image_uri: str,
    entrypoint: str,
    volumes: List[str],
    commands: List[str],
    options: Optional[str],
) -> batch_v1.types.task.Runnable:
  """Build a Runnable object for a Batch request.

  Args:
    run_in_background (bool): True for the action to run in the background
    always_run (bool): True for the action to run even in case of error from
      prior actions
    environment (Environment): Environment variables for action
    image_uri (str): Docker image path
    entrypoint (str): Docker image entrypoint path
    volumes (List[str]): List of volume mounts (host_path:container_path)
    commands (List[str]): Command arguments to pass to the entrypoint
    options (str): Container options such as "--gpus all"

  Returns:
    An object representing a Runnable
  """
  container = build_container(image_uri, entrypoint, volumes, commands, options)
  runnable = batch_v1.Runnable()
  runnable.container = container
  runnable.background = run_in_background
  runnable.always_run = always_run
  runnable.environment = environment
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


def build_gcs_volume(
    bucket: str, path: str, mount_options: List[str]
) -> batch_v1.types.Volume:
  """Build a Volume object mounted to a GCS bucket for a Batch request.

  Args:
    bucket (str): Name of bucket to mount (without the gs:// prefix)
    path (str): Path to mount the bucket at inside the container.
    mount_options (List[str]): List of mount options

  Returns:
    An object representing a Mount.
  """
  volume = batch_v1.Volume()
  volume.gcs = batch_v1.GCS(remote_path=bucket)
  volume.mount_path = path
  volume.mount_options = mount_options
  return volume


def build_network_policy(
    network: str,
    subnetwork: str,
    no_external_ip_address: bool,
) -> batch_v1.types.AllocationPolicy.NetworkPolicy:
  """Build a network policy for a Batch request.

  Args:
    network (str): The URL of an existing network resource.
    subnetwork (str): The URL of an existing subnetwork resource in the network.
    no_external_ip_address (bool): No external public IP address.

  Returns:
    An object representing a network policy.
  """
  network_policy = batch_v1.AllocationPolicy.NetworkPolicy(
      network_interfaces=[
          batch_v1.AllocationPolicy.NetworkInterface(
              network=network,
              subnetwork=subnetwork,
              no_external_ip_address=no_external_ip_address,
          )
      ]
  )
  return network_policy


def build_service_account(
    service_account_email: str,
    scopes: List[str],
) -> batch_v1.types.ServiceAccount:
  service_account = batch_v1.ServiceAccount(
      email=service_account_email,
      scopes=scopes,
  )
  return service_account


def build_allocation_policy(
    ipts: List[batch_v1.types.AllocationPolicy.InstancePolicyOrTemplate],
    service_account: batch_v1.types.ServiceAccount,
    network_policy: batch_v1.types.AllocationPolicy.NetworkPolicy,
    location_policy: batch_v1.types.AllocationPolicy.LocationPolicy,
) -> batch_v1.types.AllocationPolicy:
  allocation_policy = batch_v1.AllocationPolicy()
  allocation_policy.instances = ipts
  allocation_policy.service_account = service_account
  allocation_policy.network = network_policy
  allocation_policy.location = location_policy
  return allocation_policy


def build_instance_policy_or_template(
    instance_policy: batch_v1.types.AllocationPolicy.InstancePolicy,
    install_gpu_drivers: bool,
) -> batch_v1.types.AllocationPolicy.InstancePolicyOrTemplate:
  ipt = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
  ipt.policy = instance_policy
  ipt.install_gpu_drivers = install_gpu_drivers
  return ipt


def build_logs_policy(
    destination: batch_v1.types.LogsPolicy.Destination, logs_path: str
) -> batch_v1.types.LogsPolicy:
  logs_policy = batch_v1.LogsPolicy()
  logs_policy.destination = destination
  logs_policy.logs_path = logs_path

  return logs_policy


def build_instance_policy(
    boot_disk: batch_v1.types.AllocationPolicy.Disk,
    disks: List[batch_v1.types.AllocationPolicy.AttachedDisk],
    machine_type: str,
    accelerators: MutableSequence[batch_v1.types.AllocationPolicy.Accelerator],
    provisioning_model: batch_v1.types.AllocationPolicy.ProvisioningModel,
) -> batch_v1.types.AllocationPolicy.InstancePolicy:
  """Build an instance policy for a Batch request.

  Args:
    boot_disk (Disk): Boot disk to be created and attached to each VM by.
    disks (List[AttachedDisk]): Non-boot disks to be attached for each VM.
    machine_type (str): The Compute Engine machine type.
    accelerators (List): The accelerators attached to each VM instance.
    provisioning_model (enum): Either SPOT (preemptible) or STANDARD

  Returns:
    An object representing an instance policy.
  """
  instance_policy = batch_v1.AllocationPolicy.InstancePolicy()
  instance_policy.boot_disk = boot_disk
  instance_policy.disks = [disks]
  instance_policy.machine_type = machine_type
  instance_policy.accelerators = accelerators
  instance_policy.provisioning_model = provisioning_model

  return instance_policy


def build_attached_disk(
    disk: batch_v1.types.AllocationPolicy.Disk, device_name: str
) -> batch_v1.types.AllocationPolicy.AttachedDisk:
  attached_disk = batch_v1.AllocationPolicy.AttachedDisk()
  attached_disk.new_disk = disk
  attached_disk.device_name = device_name
  return attached_disk


def build_persistent_disk(
    size_gb: int, disk_type: str, image: str
) -> batch_v1.types.AllocationPolicy.Disk:
  disk = batch_v1.AllocationPolicy.Disk()
  disk.type = disk_type
  disk.size_gb = size_gb
  disk.image = image
  return disk


def build_accelerators(
    accelerator_type, accelerator_count
) -> MutableSequence[batch_v1.types.AllocationPolicy.Accelerator]:
  accelerators = []
  if accelerator_type:
    accelerator = batch_v1.AllocationPolicy.Accelerator()
    accelerator.count = accelerator_count
    accelerator.type = accelerator_type
    accelerators.append(accelerator)

  return accelerators


def build_location_policy(
    allowed_locations: List[str],
) -> batch_v1.types.AllocationPolicy.LocationPolicy:
  location_policy = batch_v1.AllocationPolicy.LocationPolicy()
  location_policy.allowed_locations = allowed_locations
  return location_policy