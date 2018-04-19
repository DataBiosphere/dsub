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


def build_network(name, use_private_address):
  return {
      'name': name,
      'usePrivateAddress': use_private_address,
  }


def build_disks(name, size_gb):
  return [{
      'name': name,
      'sizeGb': size_gb,
  }]


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
                  labels=None):
  """Build a VirtualMachine object for a Pipeline request.

  Args:
    network (dict): Network details for the pipeline to run in.
    machine_type (str): GCE Machine Type string for the pipeline.
    preemptible (bool): Use a preemptible VM for the job.
    service_account (dict): Service account configuration for the VM.
    boot_disk_size_gb (int): Boot disk size in GB.
    disks (list[dict]): List of disks to mount.
    labels (dict[string, string]): Labels for the VM.

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
      'labels': labels,
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


def build_pipeline(actions, resources, environment):
  """Build an Pipeline argument for a Pipeline request.

  Args:
    actions (List): A list of actions to execute.
    resources (dict): An object indicating pipeline resources.
    environment (dict[str,str]): The environment to pass into the container.

  Returns:
    An object representing a Pipelines Resource.
  """

  return {
      'actions': actions,
      'resources': resources,
      'environment': environment,
  }


if __name__ == '__main__':
  pass
