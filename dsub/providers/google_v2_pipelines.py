# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility routines for constructing a Google Genomics Pipelines v2 API request.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def build_network(name, subnetwork, use_private_address):
  return {
      'name': name,
      'subnetwork': subnetwork,
      'usePrivateAddress': use_private_address,
  }


def build_disk(name, size_gb, source_image):
  return {
      'name': name,
      'sizeGb': size_gb,
      'sourceImage': source_image,
  }


def build_accelerator(accelerator_type, accelerator_count):
  return {'type': accelerator_type, 'count': accelerator_count}


def build_service_account(email, scopes):
  return {
      'email': email,
      'scopes': scopes,
  }


def build_machine(network=None,
                  machine_type=None,
                  preemptible=None,
                  service_account=None,
                  boot_disk_size_gb=None,
                  disks=None,
                  accelerators=None,
                  labels=None,
                  cpu_platform=None):
  """Build a VirtualMachine object for a Pipeline request.

  Args:
    network (dict): Network details for the pipeline to run in.
    machine_type (str): GCE Machine Type string for the pipeline.
    preemptible (bool): Use a preemptible VM for the job.
    service_account (dict): Service account configuration for the VM.
    boot_disk_size_gb (int): Boot disk size in GB.
    disks (list[dict]): List of disks to mount.
    accelerators (list[dict]): List of accelerators to attach to the VM.
    labels (dict[string, string]): Labels for the VM.
    cpu_platform (str): The CPU platform to request.

  Returns:
    An object representing a VirtualMachine.
  """
  return {
      'network': network,
      'machineType': machine_type,
      'preemptible': preemptible,
      'serviceAccount': service_account,
      'bootDiskSizeGb': boot_disk_size_gb,
      'disks': disks,
      'accelerators': accelerators,
      'labels': labels,
      'cpuPlatform': cpu_platform,
  }


def build_resources(project=None,
                    regions=None,
                    zones=None,
                    virtual_machine=None):
  """Build a Resources object for a Pipeline request.

  Args:
    project (str): Cloud project for the Pipeline to run in.
    regions (List[str]): List of regions for the pipeline to run in.
    zones (List[str]): List of zones for the pipeline to run in.
    virtual_machine(str): Virtual machine type string.

  Returns:
    An object representing a Resource.
  """

  return {
      'projectId': project,
      'regions': regions,
      'zones': zones,
      'virtualMachine': virtual_machine,
  }


def build_mount(disk, path, read_only):
  """Build a Mount object for a Pipeline request.

  Args:
    disk (str): Name of disk to mount, as specified in the resources section.
    path (str): Path to mount the disk at inside the container.
    read_only (boolean): If true,  disk is mounted read only in the container.

  Returns:
    An object representing a Mount.
  """

  return {
      'disk': disk,
      'path': path,
      'readOnly': read_only,
  }


def build_action(name=None,
                 image_uri=None,
                 commands=None,
                 entrypoint=None,
                 environment=None,
                 pid_namespace=None,
                 flags=None,
                 port_mappings=None,
                 mounts=None,
                 labels=None):
  """Build an Action object for a Pipeline request.

  Args:
    name (str): An optional name for the container.
    image_uri (str): The URI to pull the container image from.
    commands (List[str]): commands and arguments to run inside the container.
    entrypoint (str): overrides the ENTRYPOINT specified in the container.
    environment (dict[str,str]): The environment to pass into the container.
    pid_namespace (str): The PID namespace to run the action inside.
    flags (str): Flags that control the execution of this action.
    port_mappings (dict[int, int]): A map of container to host port mappings for
      this container.
    mounts (List): A list of mounts to make available to the action.
    labels (dict[str]): Labels to associate with the action.

  Returns:
    An object representing an Action resource.
  """

  return {
      'name': name,
      'imageUri': image_uri,
      'commands': commands,
      'entrypoint': entrypoint,
      'environment': environment,
      'pidNamespace': pid_namespace,
      'flags': flags,
      'portMappings': port_mappings,
      'mounts': mounts,
      'labels': labels,
  }


def build_pipeline(actions, resources, environment, timeout):
  """Build an Pipeline argument for a Pipeline request.

  Args:
    actions (List): A list of actions to execute.
    resources (dict): An object indicating pipeline resources.
    environment (dict[str,str]): The environment to pass into the container.
    timeout (str): A duration in seconds with up to nine fractional digits,
      terminated by 's'.

  Returns:
    An object representing a Pipelines Resource.
  """

  return {
      'actions': actions,
      'resources': resources,
      'environment': environment,
      'timeout': timeout,
  }


if __name__ == '__main__':
  pass
