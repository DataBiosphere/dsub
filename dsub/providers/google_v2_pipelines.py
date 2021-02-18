# Lint as: python3
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

from . import google_v2_versions

_API_VERSION = None


def set_api_version(api_version):
  assert api_version in (google_v2_versions.V2ALPHA1, google_v2_versions.V2BETA)

  global _API_VERSION
  _API_VERSION = api_version


def build_network(name, subnetwork, use_private_address):
  if _API_VERSION == google_v2_versions.V2ALPHA1:
    network_key = 'name'
  elif _API_VERSION == google_v2_versions.V2BETA:
    network_key = 'network'
  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)

  return {
      network_key: name,
      'subnetwork': subnetwork,
      'usePrivateAddress': use_private_address,
  }


def build_disk(name, size_gb, source_image, disk_type):
  return {
      'name': name,
      'sizeGb': size_gb,
      'type': disk_type,
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
                  cpu_platform=None,
                  nvidia_driver_version=None,
                  enable_stackdriver_monitoring=None):
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
    nvidia_driver_version (str): The NVIDIA driver version to use when attaching
      an NVIDIA GPU accelerator.
    enable_stackdriver_monitoring (bool): Enable stackdriver monitoring
      on the VM.

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
      'nvidiaDriverVersion': nvidia_driver_version,
      'enableStackdriverMonitoring': enable_stackdriver_monitoring,
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

  resources = {
      'regions': regions,
      'zones': zones,
      'virtualMachine': virtual_machine,
  }

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    resources['projectId'] = project

  return resources


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
                 port_mappings=None,
                 mounts=None,
                 labels=None,
                 always_run=None,
                 enable_fuse=None,
                 run_in_background=None,
                 block_external_network=None):
  """Build an Action object for a Pipeline request.

  Args:
    name (str): An optional name for the container.
    image_uri (str): The URI to pull the container image from.
    commands (List[str]): commands and arguments to run inside the container.
    entrypoint (str): overrides the ENTRYPOINT specified in the container.
    environment (dict[str,str]): The environment to pass into the container.
    pid_namespace (str): The PID namespace to run the action inside.
    port_mappings (dict[int, int]): A map of container to host port mappings for
      this container.
    mounts (List): A list of mounts to make available to the action.
    labels (dict[str]): Labels to associate with the action.
    always_run (bool): Action must run even if pipeline has already failed.
    enable_fuse (bool): Enable access to the FUSE device for this action.
    run_in_background (bool): Allow the action to run in the background.
    block_external_network (bool): Prevents the container from accessing the
      external network.

  Returns:
    An object representing an Action resource.
  """

  action = {
      'imageUri': image_uri,
      'commands': commands,
      'entrypoint': entrypoint,
      'environment': environment,
      'pidNamespace': pid_namespace,
      'portMappings': port_mappings,
      'mounts': mounts,
      'labels': labels,
  }

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    action['name'] = name

    # In v2alpha1, the flags are passed as a list of strings
    flags = []
    if always_run:
      flags.append('ALWAYS_RUN')
    if enable_fuse:
      flags.append('ENABLE_FUSE')
    if run_in_background:
      flags.append('RUN_IN_BACKGROUND')
    if block_external_network:
      flags.append('BLOCK_EXTERNAL_NETWORK')

    if flags:
      action['flags'] = flags

  elif _API_VERSION == google_v2_versions.V2BETA:
    action['containerName'] = name

    # In v2beta, the flags are direct members of the action
    action['alwaysRun'] = always_run
    action['enableFuse'] = enable_fuse
    action['runInBackground'] = run_in_background
    action['blockExternalNetwork'] = block_external_network

  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)

  return action


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
