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
"""Class definitions for dsub jobs.

The dsub object model is based around jobs and tasks.

A dsub job specified exclusively with command-line arguments contains one
implicit task (task-id: None).
A dsub job launched with the --tasks flag will contain one or more
explicitly identified tasks (task-id: <n>).

dsub jobs are made up of:
- metadata: job-id, user-id, create-time, etc.
- params: labels, envs, inputs, outputs
- resources: logging-uri, min-cpu, min-ram, etc.

tasks are made up of
- metadata: task-id, task-attempt (only when retries!=0)
- params: labels, envs, inputs, outputs
- resources: logging-uri, min-cpu, min-ram, etc.

The top-level object is the JobDescriptor, which contains:
  job_metadata: A dict of metadata values.
  job_params: A dict of parameter values.
  job_resources: A Resources object.
  An array of TaskDescriptors.

(That the job_metadata and job_params are not well-defined objects is
historical, rather than intentional.)

Each TaskDescriptor contains:
  task_metadata: A dict of metadata values.
  task_params: A dict of parameter values.
  task_resources: A Resources object.

The object model here is presently more complete than what the user-interface
allows. For example, min-cpu, min-ram, and other resources are not supported
in the --tasks file, but the object model allows for callers using the Python
API to set each of the resource fields at the task level.
"""

from __future__ import print_function

import collections
import re
import string

from . import dsub_util
import pytz
import yaml

DEFAULT_MIN_CORES = 1
DEFAULT_MIN_RAM = 3.75
DEFAULT_MACHINE_TYPE = 'n1-standard-1'
DEFAULT_DISK_SIZE = 200
DEFAULT_BOOT_DISK_SIZE = 10
DEFAULT_MOUNTED_DISK_SIZE = 10
DEFAULT_PREEMPTIBLE = False
DEFAULT_DISK_TYPE = 'pd-standard'
DEFAULT_LOCATION = 'us-central1'

# Users may specify their own labels, however dsub also uses an implicit set of
# labels (in the Google providers). Reserve these labels such that users do
# not attempt to set them.
RESERVED_LABELS = frozenset([
    'job-name', 'job-id', 'user-id', 'task-id', 'dsub-version', 'task-attempt'
])

P_LOCAL = 'local'
P_GCS = 'google-cloud-storage'
FILE_PROVIDERS = frozenset([P_LOCAL, P_GCS])


class Script(object):
  """Script to be run by for the job.

  The Pipeline's API specifically supports bash commands as the docker
  command. To support any type of script (Python, Ruby, etc.), the contents
  are uploaded as a simple environment variable input parameter.
  The docker command then writes the variable contents to a file and
  executes it.

  Attributes:
    name: (str) File name of this script.
    value: (str) Content of this script.
  """

  def __init__(self, name, value):
    self.name = name
    self.value = value


def validate_param_name(name, param_type):
  """Validate that the name follows posix conventions for env variables."""
  # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_235
  #
  # 3.235 Name
  # In the shell command language, a word consisting solely of underscores,
  # digits, and alphabetics from the portable character set.
  if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
    raise ValueError('Invalid %s: %s' % (param_type, name))


def validate_bucket_name(bucket):
  """Validate that the name is a valid GCS bucket."""
  if not bucket.startswith('gs://'):
    raise ValueError(
        'Invalid bucket path "%s". Must start with "gs://".' % bucket)
  bucket_name = bucket[len('gs://'):]
  if not re.search(r'^\w[\w_\.-]{1,61}\w$', bucket_name):
    raise ValueError('Invalid bucket name: %s' % bucket)


class UriParts(str):
  """Subclass string for multipart URIs.

  This string subclass is used for URI references. The path and basename
  attributes are used to maintain separation of this information in cases where
  it might otherwise be ambiguous. The value of a UriParts string is a URI.

  Attributes:
    path: Strictly speaking, the path attribute is the entire leading part of
      a URI (including scheme, host, and path). This attribute defines the
      hierarchical location of a resource. Path must end in a forward
      slash. Local file URIs are represented as relative URIs (path only).
    basename: The last token of a path that follows a forward slash. Generally
      this defines a specific resource or a pattern that matches resources. In
      the case of URI's that consist only of a path, this will be empty.

  Examples:
    | uri                         |  uri.path              | uri.basename  |
    +-----------------------------+------------------------+---------------|
    | gs://bucket/folder/file.txt | 'gs://bucket/folder/'  | 'file.txt'    |
    | http://example.com/1.htm    | 'http://example.com/'  | '1.htm'       |
    | /tmp/tempdir1/              | '/tmp/tempdir1/'       | ''            |
    | /tmp/ab.txt                 | '/tmp/'                | 'ab.txt'      |
  """

  def __new__(cls, path, basename):
    basename = basename if basename is not None else ''
    newuri = str.__new__(cls, path + basename)
    newuri.path = path
    newuri.basename = basename
    return newuri


class EnvParam(collections.namedtuple('EnvParam', ['name', 'value'])):
  """Name/value input parameter to a pipeline.

  Attributes:
    name (str): the input parameter and environment variable name.
    value (str): the variable value (optional).
  """
  __slots__ = ()

  def __new__(cls, name, value=None):
    validate_param_name(name, 'Environment variable')
    return super(EnvParam, cls).__new__(cls, name, value)


class LoggingParam(
    collections.namedtuple('LoggingParam', ['uri', 'file_provider'])):
  """File parameter used for logging.

  Attributes:
    uri (UriParts): A uri or local file path.
    file_provider (enum): Service or infrastructure hosting the file.
  """
  pass


def convert_to_label_chars(s):
  """Turn the specified name and value into a valid Google label."""

  # We want the results to be user-friendly, not just functional.
  # So we can't base-64 encode it.
  #   * If upper-case: lower-case it
  #   * If the char is not a standard letter or digit. make it a dash

  # March 2019 note: underscores are now allowed in labels.
  # However, removing the conversion of underscores to dashes here would
  # create inconsistencies between old jobs and new jobs.
  # With existing code, $USER "jane_doe" has a user-id label of "jane-doe".
  # If we remove the conversion, the user-id label for new jobs is "jane_doe".
  # This makes looking up old jobs more complicated.

  accepted_characters = string.ascii_lowercase + string.digits + '-'

  def label_char_transform(char):
    if char in accepted_characters:
      return char
    if char in string.ascii_uppercase:
      return char.lower()
    return '-'

  return ''.join(label_char_transform(c) for c in s)


class LabelParam(collections.namedtuple('LabelParam', ['name', 'value'])):
  """Name/value label parameter to a pipeline.

  Subclasses of LabelParam may flip the _allow_reserved_keys attribute in order
  to allow reserved label values to be used. The check against reserved keys
  ensures that providers can rely on the label system to track dsub-related
  values without allowing users to accidentally overwrite the labels.

  Attributes:
    name (str): the label name.
    value (str): the label value (optional).
  """
  _allow_reserved_keys = False
  __slots__ = ()

  def __new__(cls, name, value=None):
    cls._validate_label(name, value)
    return super(LabelParam, cls).__new__(cls, name, value)

  @classmethod
  def _validate_label(cls, name, value):
    """Raise ValueError if the label is invalid."""
    # Rules for labels are described in:
    #  https://cloud.google.com/compute/docs/labeling-resources#restrictions

    # * Keys and values cannot be longer than 63 characters each.
    # * Keys and values can only contain lowercase letters, numeric characters,
    #   underscores, and dashes.
    # * International characters are allowed.
    # * Label keys must start with a lowercase letter and international
    #   characters are allowed.
    # * Label keys cannot be empty.
    cls._check_label_name(name)
    cls._check_label_value(value)

    # Ensure that reserved labels are not being used.
    if not cls._allow_reserved_keys and name in RESERVED_LABELS:
      raise ValueError('Label flag (%s=...) must not use reserved keys: %r' %
                       (name, list(RESERVED_LABELS)))

  @staticmethod
  def _check_label_name(name):
    if len(name) < 1 or len(name) > 63:
      raise ValueError('Label name must be 1-63 characters long: "%s"' % name)
    if not re.match(r'^[a-z]([-_a-z0-9]*)?$', name):
      raise ValueError(
          'Invalid name for label: "%s". Must start with a lowercase letter '
          'and contain only lowercase letters, numeric characters, '
          'underscores, and dashes.' % name)

  @staticmethod
  def _check_label_value(value):
    if not value:
      return

    if len(value) > 63:
      raise ValueError(
          'Label values must not be longer than 63 characters: "%s"' % value)

    if not re.match(r'^([-_a-z0-9]*)?$', value):
      raise ValueError(
          'Invalid value for label: "%s". Must contain only lowercase letters, '
          'numeric characters, underscores, and dashes.' % value)


class FileParam(
    collections.namedtuple('FileParam', [
        'name',
        'value',
        'docker_path',
        'uri',
        'recursive',
        'file_provider',
        'disk_size',
        'disk_type',
    ])):
  """File parameter to be automatically localized or de-localized.

  Input files are automatically localized to the pipeline VM's local disk.

  Output files are automatically de-localized to a remote URI from the
  pipeline VM's local disk.

  Attributes:
    name (str): the parameter and environment variable name.
    value (str): the original value given by the user on the command line or
                 in the TSV file.
    docker_path (str): the on-VM location; also set as the environment variable
                       value.
    uri (UriParts): A uri or local file path.
    recursive (bool): Whether recursive copy is wanted.
    file_provider (enum): Service or infrastructure hosting the file.
    disk_size (int): Size in Gb for a mounted Google Persistent Disk.
    disk_type (string): Disk type for a mounted Google Persistent Disk.
  """
  __slots__ = ()

  def __new__(
      cls,
      name,
      value=None,
      docker_path=None,
      uri=None,
      recursive=False,
      file_provider=None,
      disk_size=None,
      disk_type=None,
  ):
    return super(FileParam,
                 cls).__new__(cls, name, value, docker_path, uri, recursive,
                              file_provider, disk_size, disk_type)


class InputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False,
              file_provider=None,
              disk_size=None,
              disk_type=None):
    validate_param_name(name, 'Input parameter')
    return super(InputFileParam,
                 cls).__new__(cls, name, value, docker_path, uri, recursive,
                              file_provider, disk_size, disk_type)


class OutputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False,
              file_provider=None,
              disk_size=None,
              disk_type=None):
    validate_param_name(name, 'Output parameter')
    return super(OutputFileParam,
                 cls).__new__(cls, name, value, docker_path, uri, recursive,
                              file_provider, disk_size, disk_type)


class MountParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value,
              docker_path=None,
              uri=None,
              disk_size=None,
              disk_type=None):
    validate_param_name(name, 'Mount parameter')
    return super(MountParam, cls).__new__(
        cls,
        name,
        value,
        docker_path,
        uri,
        disk_size=disk_size,
        disk_type=disk_type)


class GCSMountParam(MountParam):
  """A MountParam representing a Cloud Storage bucket to mount via gcsfuse."""

  def __new__(cls, name, value, docker_path):
    validate_bucket_name(value)
    return super(GCSMountParam, cls).__new__(cls, name, value, docker_path)


class PersistentDiskMountParam(MountParam):
  """A MountParam representing a Google Persistent Disk."""

  def __new__(cls, name, value, docker_path, disk_size, disk_type):
    return super(PersistentDiskMountParam, cls).__new__(
        cls, name, value, docker_path, disk_size=disk_size, disk_type=disk_type)


class LocalMountParam(MountParam):
  """A MountParam representing a path on the local machine."""

  def __new__(cls, name, value, docker_path, uri):
    return super(LocalMountParam, cls).__new__(cls, name, value, docker_path,
                                               uri)


class Resources(
    collections.namedtuple('Resources', [
        'min_cores',
        'min_ram',
        'machine_type',
        'disk_size',
        'disk_type',
        'boot_disk_size',
        'preemptible',
        'image',
        'logging',
        'logging_path',
        'regions',
        'zones',
        'service_account',
        'scopes',
        'cpu_platform',
        'network',
        'subnetwork',
        'use_private_address',
        'accelerator_type',
        'accelerator_count',
        'nvidia_driver_version',
        'timeout',
        'log_interval',
        'ssh',
        'enable_stackdriver_monitoring',
        'max_retries',
        'max_preemptible_attempts',
    ])):
  """Job resource parameters related to CPUs, memory, and disk.

  Attributes:
    min_cores (int): number of CPU cores
    min_ram (float): amount of memory (in GB)
    machine_type (str): machine type (e.g. 'n1-standard-1', 'custom-1-4096')
    disk_size (int): size of the data disk (in GB)
    disk_type (string): Set the disk type of the data disk
    boot_disk_size (int): size of the boot disk (in GB)
    preemptible (bool): use a preemptible VM for the job
    image (str): Docker image name
    logging (param_util.LoggingParam): user-specified location for jobs logs
    logging_path (param_util.LoggingParam): resolved location for jobs logs
    regions (List[str]): region list in which to run the job
    zones (List[str]): zone list in which to run the job
    service_account (string): Email address of the service account to be
      authorized on the Compute Engine VM for each job task.
    scopes (List[str]): OAuth2 scopes for the job
    cpu_platform (string): The CPU platform to request (e.g. 'Intel Skylake')
    network (string): The network name to attach the VM's network interface to.
    subnetwork (string): The name of the subnetwork to attach the instance to.
    use_private_address (bool): Do not attach a public IP address to the VM
    accelerator_type (string): Accelerator type (e.g. 'nvidia-tesla-k80').
    accelerator_count (int): Number of accelerators of the specified type to
      attach.
    nvidia_driver_version (string): The NVIDIA driver version to use when
      attaching an NVIDIA GPU accelerator.
    timeout (string): The max amount of time to give the pipeline to complete.
    log_interval (string): The amount of time to sleep between log uploads.
    ssh (bool): Start an SSH container in the background.
    enable_stackdriver_monitoring (bool): Enable stackdriver monitoring
      on the VM.
    max_retries (int): Maximum allowed number of retry attempts.
    max_preemptible_attempts (param_util.PreemptibleParam): Int representing
      maximum allowed number of attempts on a preemptible machine, or boolean
      representing always preemtible.
  """
  __slots__ = ()

  def __new__(cls,
              min_cores=None,
              min_ram=None,
              machine_type=None,
              disk_size=None,
              disk_type=None,
              boot_disk_size=None,
              preemptible=None,
              image=None,
              logging=None,
              logging_path=None,
              regions=None,
              zones=None,
              service_account=None,
              scopes=None,
              cpu_platform=None,
              network=None,
              subnetwork=None,
              use_private_address=None,
              accelerator_type=None,
              accelerator_count=0,
              nvidia_driver_version=None,
              timeout=None,
              log_interval=None,
              ssh=None,
              enable_stackdriver_monitoring=None,
              max_retries=None,
              max_preemptible_attempts=None):
    return super(Resources,
                 cls).__new__(cls, min_cores, min_ram, machine_type, disk_size,
                              disk_type, boot_disk_size, preemptible, image,
                              logging, logging_path, regions, zones,
                              service_account, scopes, cpu_platform, network,
                              subnetwork, use_private_address, accelerator_type,
                              accelerator_count, nvidia_driver_version, timeout,
                              log_interval, ssh, enable_stackdriver_monitoring,
                              max_retries, max_preemptible_attempts)


def ensure_job_params_are_complete(job_params):
  """For the job, ensure that each param entry is not None."""
  for param in [
      'labels', 'envs', 'inputs', 'outputs', 'mounts', 'input-recursives',
      'output-recursives'
  ]:
    if not job_params.get(param):
      job_params[param] = set()


def ensure_task_params_are_complete(task_descriptors):
  """For each task, ensure that each task param entry is not None."""
  for task_desc in task_descriptors:
    for param in [
        'labels', 'envs', 'inputs', 'outputs', 'input-recursives',
        'output-recursives'
    ]:
      if not task_desc.task_params.get(param):
        task_desc.task_params[param] = set()


def _remove_empty_items(d, required):
  """Return a new dict with any empty items removed.

  Note that this is not a deep check. If d contains a dictionary which
  itself contains empty items, those are never checked.

  This method exists to make to_serializable() functions cleaner.
  We could revisit this some day, but for now, the serialized objects are
  stripped of empty values to keep the output YAML more compact.

  Args:
    d: a dictionary
    required: list of required keys (for example, TaskDescriptors always emit
      the "task-id", even if None)

  Returns:
    A dictionary with empty items removed.
  """

  new_dict = {}
  for k, v in d.items():
    if k in required:
      new_dict[k] = v
    elif isinstance(v, int) or v:
      # "if v" would suppress emitting int(0)
      new_dict[k] = v

  return new_dict


class TaskDescriptor(object):
  """Metadata, resources, and parameters for a dsub task.

  A TaskDescriptor on its own is incomplete and should always be handled in
  the context of a JobDescriptor (described below).

  Args:
    task_metadata: Task metadata such as task-id.
    task_params: Task parameters such as labels, envs, inputs, and outputs.
    task_resources: Resources specified such as ram, cpu, and logging path.
  """

  def __init__(self, task_metadata, task_params, task_resources):
    self.task_metadata = task_metadata
    self.task_params = task_params
    self.task_resources = task_resources

  @classmethod
  def get_complete_descriptor(cls, task_metadata, task_params, task_resources):
    task_descriptor = cls(task_metadata, task_params, task_resources)
    ensure_task_params_are_complete([task_descriptor])
    return task_descriptor

  def __str__(self):
    return 'task-id: {}'.format(self.job_metadata.get('task-id'))

  def __repr__(self):
    return ('task_metadata: {}, task_params: {}').format(
        repr(self.task_metadata), repr(self.task_params))

  def to_serializable(self):
    """Return a dict populated for serialization (as YAML/JSON)."""

    task_metadata = self.task_metadata
    task_params = self.task_params
    task_resources = self.task_resources

    # The only required field is the task-id, even if it is None
    task_id = None
    if task_metadata.get('task-id') is not None:
      task_id = str(task_metadata.get('task-id'))

    task = {'task-id': task_id}
    task['create-time'] = task_metadata.get('create-time')
    task['task-attempt'] = task_metadata.get('task-attempt')

    if task_resources.logging_path:
      task['logging-path'] = str(task_resources.logging_path.uri)

    task['labels'] = {var.name: var.value for var in task_params['labels']}

    task['envs'] = {var.name: var.value for var in task_params['envs']}

    task['inputs'] = {
        var.name: var.value
        for var in task_params['inputs']
        if not var.recursive
    }
    task['input-recursives'] = {
        var.name: var.value
        for var in task_params['inputs']
        if var.recursive
    }
    task['outputs'] = {
        var.name: var.value
        for var in task_params['outputs']
        if not var.recursive
    }
    task['output-recursives'] = {
        var.name: var.value
        for var in task_params['outputs']
        if var.recursive
    }

    return _remove_empty_items(task, ['task-id'])


class JobDescriptor(object):
  """Metadata, resources, and parameters for a dsub job.

  Args:
    job_metadata: Job metadata such as job-id, job-name, and user-id.
    job_params: Job parameters such as labels, envs, inputs, and outputs.
    job_resources: Resources specified such as ram, cpu, and logging path.
    task_descriptors: Task metadata, parameters, and resources.
  """

  def __init__(self, job_metadata, job_params, job_resources, task_descriptors):
    self.job_metadata = job_metadata
    self.job_params = job_params
    self.job_resources = job_resources
    self.task_descriptors = task_descriptors

  @classmethod
  def get_complete_descriptor(cls, job_metadata, job_params, job_resources,
                              task_descriptors):
    ensure_job_params_are_complete(job_params)
    job_descriptor = cls(job_metadata, job_params, job_resources,
                         task_descriptors)
    return job_descriptor

  def __str__(self):
    return 'job-id: {}'.format(self.job_metadata.get('job-id'))

  def __repr__(self):
    return ('job_metadata: {}, job_params: {}, job_resources: {}, '
            'task_descriptors: {}').format(
                repr(self.job_metadata), repr(self.job_params),
                repr(self.job_resources), repr(self.task_descriptors))

  def to_serializable(self):
    """Return a dict populated for serialization (as YAML/JSON)."""

    job_metadata = self.job_metadata
    job_resources = self.job_resources
    job_params = self.job_params
    task_descriptors = self.task_descriptors

    job = {
        'job-id': job_metadata.get('job-id'),
        'job-name': job_metadata.get('job-name'),
        'user-id': job_metadata.get('user-id'),
        'create-time': job_metadata.get('create-time'),
        'dsub-version': job_metadata.get('dsub-version'),
        'user-project': job_metadata.get('user-project'),
        'task-ids': job_metadata.get('task-ids'),
        'script-name': job_metadata['script'].name,
    }

    # logging is specified as a command-line argument and is typically
    # transformed (substituting job-id). The transformed value is saved
    # on a per-task basis as the 'logging-path'.
    if job_resources.logging:
      job['logging'] = str(job_resources.logging.uri)

    job['labels'] = {var.name: var.value for var in job_params['labels']}

    job['envs'] = {var.name: var.value for var in job_params['envs']}

    job['inputs'] = {
        var.name: var.value
        for var in job_params['inputs']
        if not var.recursive
    }
    job['input-recursives'] = {
        var.name: var.value
        for var in job_params['inputs']
        if var.recursive
    }

    job['outputs'] = {
        var.name: var.value
        for var in job_params['outputs']
        if not var.recursive
    }
    job['output-recursives'] = {
        var.name: var.value
        for var in job_params['outputs']
        if var.recursive
    }
    job['mounts'] = {var.name: var.value for var in job_params['mounts']}

    tasks = []
    for task_descriptor in task_descriptors:
      tasks.append(task_descriptor.to_serializable())

    job['tasks'] = tasks

    return _remove_empty_items(job, [])

  def to_yaml(self):
    """Return a YAML string representing the job and task data.

    A provider's internal representation of a dsub task typically does not map
    1-1 to the dsub representation. For example, the Google Genomics Pipeline
    does not natively support "input-recursive" or "output-recursive", so the
    google provider cannot easily reconstruct the user inputs from the
    pipeline's associated Operation object.

    All providers are likely to need a way to reliably serialize job and task-
    related information, either for dstat or for any type of "retry" mechanism
    we might want to build.

    Returns:
      YAML string
    """
    return yaml.dump(self.to_serializable(), default_flow_style=False)

  @classmethod
  def _label_params_from_dict(cls, raw_labels):
    labels = set()
    for key in raw_labels:
      labels.add(LabelParam(key, raw_labels.get(key)))
    return labels

  @classmethod
  def _env_params_from_dict(cls, raw_envs):
    envs = set()
    for key in raw_envs:
      envs.add(EnvParam(key, raw_envs.get(key)))
    return envs

  @classmethod
  def _input_file_params_from_dict(cls, raw_inputs, recursive):
    inputs = set()
    for key in raw_inputs:
      inputs.add(InputFileParam(key, raw_inputs.get(key), recursive=recursive))
    return inputs

  @classmethod
  def _output_file_params_from_dict(cls, raw_outputs, recursive):
    outputs = set()
    for key in raw_outputs:
      outputs.add(
          OutputFileParam(key, raw_outputs.get(key), recursive=recursive))
    return outputs

  @classmethod
  def _mount_params_from_dict(cls, raw_mounts):
    mounts = set()
    for key in raw_mounts:
      mounts.add(MountParam(key, raw_mounts.get(key)))
    return mounts

  @classmethod
  def _set_metadata_create_time(cls, metadata, create_time):
    if dsub_util.datetime_is_timezone_aware(create_time):
      # In yaml version starting at 5.3,
      # timestamps are already loaded as timezone aware
      metadata['create-time'] = create_time
    else:
      metadata['create-time'] = dsub_util.replace_timezone(
          create_time, pytz.utc)

  @classmethod
  def from_yaml(cls, yaml_string):
    """Populate and return a JobDescriptor from a YAML string."""
    try:
      job = yaml.full_load(yaml_string)
    except AttributeError:
      # For installations that cannot update their PyYAML version
      job = yaml.load(yaml_string)

    job_metadata = {}
    for key in [
        'job-id', 'job-name', 'task-ids', 'user-id', 'dsub-version',
        'user-project', 'script-name'
    ]:
      if job.get(key) is not None:
        job_metadata[key] = job.get(key)

    # Make sure that create-time string is turned into a datetime
    job_create_time = job.get('create-time')
    cls._set_metadata_create_time(job_metadata, job_create_time)

    job_resources = Resources(logging=job.get('logging'))

    job_params = {}
    job_params['labels'] = cls._label_params_from_dict(job.get('labels', {}))
    job_params['envs'] = cls._env_params_from_dict(job.get('envs', {}))
    job_params['inputs'] = cls._input_file_params_from_dict(
        job.get('inputs', {}), False)
    job_params['input-recursives'] = cls._input_file_params_from_dict(
        job.get('input-recursives', {}), True)
    job_params['outputs'] = cls._output_file_params_from_dict(
        job.get('outputs', {}), False)
    job_params['output-recursives'] = cls._output_file_params_from_dict(
        job.get('output-recursives', {}), True)
    job_params['mounts'] = cls._mount_params_from_dict(job.get('mounts', {}))

    task_descriptors = []
    for task in job.get('tasks', []):
      task_metadata = {'task-id': task.get('task-id')}

      # Old instances of the meta.yaml do not have a task create time.
      create_time = task.get('create-time')
      if create_time:
        cls._set_metadata_create_time(task_metadata, create_time)

      if task.get('task-attempt') is not None:
        task_metadata['task-attempt'] = task.get('task-attempt')

      task_params = {}
      task_params['labels'] = cls._label_params_from_dict(
          task.get('labels', {}))
      task_params['envs'] = cls._env_params_from_dict(task.get('envs', {}))
      task_params['inputs'] = cls._input_file_params_from_dict(
          task.get('inputs', {}), False)
      task_params['input-recursives'] = cls._input_file_params_from_dict(
          task.get('input-recursives', {}), True)
      task_params['outputs'] = cls._output_file_params_from_dict(
          task.get('outputs', {}), False)
      task_params['output-recursives'] = cls._output_file_params_from_dict(
          task.get('output-recursives', {}), True)

      task_resources = Resources(logging_path=task.get('logging-path'))

      task_descriptors.append(
          TaskDescriptor(task_metadata, task_params, task_resources))

    return JobDescriptor(job_metadata, job_params, job_resources,
                         task_descriptors)

  def find_task_descriptor(self, task_id):
    """Returns the task_descriptor corresponding to task_id."""

    # It is not guaranteed that the index will be task_id - 1 when --tasks is
    # used with a min/max range.
    for task_descriptor in self.task_descriptors:
      if task_descriptor.task_metadata.get('task-id') == task_id:
        return task_descriptor
    return None


def task_view_generator(job_descriptor):
  """Generator that yields a task-specific view of the job.

  This generator exists to make it easy for callers to iterate over the tasks
  in a JobDescriptor. Each pass yields a new JobDescriptor with a single task.

  Args:
    job_descriptor: A JobDescriptor with 1 or more tasks.

  Yields:
    A JobDescriptor with a single task.
  """
  for task_descriptor in job_descriptor.task_descriptors:
    jd = JobDescriptor(job_descriptor.job_metadata, job_descriptor.job_params,
                       job_descriptor.job_resources, [task_descriptor])
    yield jd
