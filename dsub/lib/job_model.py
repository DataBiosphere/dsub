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
- metadata: task-id
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
import datetime
import re
from . import dsub_util
from dateutil.tz import tzlocal
import pytz
import yaml

DEFAULT_MIN_CORES = 1
DEFAULT_MIN_RAM = 3.75
DEFAULT_DISK_SIZE = 200
DEFAULT_BOOT_DISK_SIZE = 10
DEFAULT_PREEMPTIBLE = False
DEFAULT_SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
]

# Users may specify their own labels, however dsub also uses an implicit set of
# labels (in the google provider). Reserve these labels such that users do
# not attempt to set them.
RESERVED_LABELS = frozenset(
    ['job-name', 'job-id', 'user-id', 'task-id', 'dsub-version'])

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


class LabelParam(collections.namedtuple('LabelParam', ['name', 'value'])):
  """Name/value label parameter to a pipeline.

  Subclasses of LabelParam may flip the _allow_reserved_keys attribute in order
  to allow reserved label values to be used. The check against reserved keys
  ensures that providers can rely on the label system to track dsub-related
  values without allowing users to accidentially overwrite the labels.

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
  """
  __slots__ = ()

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False,
              file_provider=None):
    return super(FileParam, cls).__new__(cls, name, value, docker_path, uri,
                                         recursive, file_provider)


class InputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False,
              file_provider=None):
    validate_param_name(name, 'Input parameter')
    return super(InputFileParam, cls).__new__(cls, name, value, docker_path,
                                              uri, recursive, file_provider)


class OutputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              uri=None,
              recursive=False,
              file_provider=None):
    validate_param_name(name, 'Output parameter')
    return super(OutputFileParam, cls).__new__(cls, name, value, docker_path,
                                               uri, recursive, file_provider)


class Resources(
    collections.namedtuple('Resources', [
        'min_cores', 'min_ram', 'disk_size', 'boot_disk_size', 'preemptible',
        'image', 'logging', 'logging_path', 'zones', 'scopes', 'keep_alive',
        'accelerator_type', 'accelerator_count'
    ])):
  """Job resource parameters related to CPUs, memory, and disk.

  Attributes:
    min_cores (int): number of CPU cores
    min_ram (float): amount of memory (in GB)
    disk_size (int): size of the data disk (in GB)
    boot_disk_size (int): size of the boot disk (in GB)
    preemptible (bool): use a preemptible VM for the job
    image (str): Docker image name
    logging (param_util.LoggingParam): user-specified location for jobs logs
    logging_path (param_util.LoggingParam): resolved location for jobs logs
    zones (str): location in which to run the job
    scopes (list): OAuth2 scopes for the job
    keep_alive (int): Seconds to keep VM alive on failure
    accelerator_type (string): Accelerator type (e.g. 'nvidia-tesla-k80').
    accelerator_count (int): Number of accelerators of the specified type to
      attach.
  """
  __slots__ = ()

  def __new__(cls,
              min_cores=None,
              min_ram=None,
              disk_size=None,
              boot_disk_size=None,
              preemptible=None,
              image=None,
              logging=None,
              logging_path=None,
              zones=None,
              scopes=None,
              keep_alive=None,
              accelerator_type=None,
              accelerator_count=0):
    return super(Resources, cls).__new__(
        cls, min_cores, min_ram, disk_size, boot_disk_size, preemptible, image,
        logging, logging_path, zones, scopes, keep_alive, accelerator_type,
        accelerator_count)


def ensure_job_params_are_complete(job_params):
  """For the job, ensure that each param entry is not None."""
  for param in 'labels', 'envs', 'inputs', 'outputs':
    if not job_params.get(param):
      job_params[param] = set()


def ensure_task_params_are_complete(task_descriptors):
  """For each task, ensure that each task param entry is not None."""
  for task_desc in task_descriptors:
    for param in 'labels', 'envs', 'inputs', 'outputs':
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
  for k, v in d.iteritems():
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
    }

    job['task-ids'] = job_metadata.get('task-ids')

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

    tasks = []
    for task_descriptor in task_descriptors:
      tasks.append(task_descriptor.to_serializable())

    job['tasks'] = tasks

    return _remove_empty_items(job, [])

  def to_yaml(self):
    """Return a YAML string representing the job and task data.

    A provider's internal represesentation of a dsub task typically does not map
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
  def _from_yaml_v0(cls, job):
    """Populate a JobDescriptor from the local provider's original meta.yaml.

    The local job provider had the first incarnation of a YAML file for each
    task. That idea was extended here in the JobDescriptor and the local
    provider adopted the JobDescriptor.to_yaml() call to write its meta.yaml.

    The JobDescriptor.from_yaml() detects if it receives a local provider's
    "v0" meta.yaml and calls this function.

    Args:
      job: an object produced from decoding meta.yaml.

    Returns:
      A JobDescriptor populated as best we can from the old meta.yaml.
    """

    # The v0 meta.yaml only contained:
    #   create-time, job-id, job-name, logging, task-id
    #   labels, envs, inputs, outputs
    # It did NOT contain user-id.
    # dsub-version might be there as a label.

    job_metadata = {}
    for key in ['job-id', 'job-name', 'create-time']:
      job_metadata[key] = job.get(key)

    # Make sure that create-time string is turned into a datetime
    job_metadata['create-time'] = dsub_util.replace_timezone(
        datetime.datetime.strptime(job['create-time'], '%Y-%m-%d %H:%M:%S.%f'),
        tzlocal())

    # The v0 meta.yaml contained a "logging" field which was the task-specific
    # logging path. It did not include the actual "--logging" value the user
    # specified.
    job_resources = Resources()

    # The v0 meta.yaml represented a single task.
    # It did not distinguish whether params were job params or task params.
    # We will treat them as either all job params or all task params, based on
    # whether the task-id is empty or an integer value.
    #
    # We also cannot distinguish whether inputs/outputs were recursive or not.
    # Just treat them all as non-recursive.
    params = {}

    # The dsub-version may be in the meta.yaml as a label. If so remove it
    # and set it as a top-level job metadata value.
    labels = job.get('labels', {})
    if 'dsub-version' in labels:
      job_metadata['dsub-version'] = labels['dsub-version']
      del labels['dsub-version']
    params['labels'] = cls._label_params_from_dict(labels)

    params['envs'] = cls._env_params_from_dict(job.get('envs', {}))
    params['inputs'] = cls._input_file_params_from_dict(
        job.get('inputs', {}), False)
    params['outputs'] = cls._output_file_params_from_dict(
        job.get('outputs', {}), False)

    if job.get('task-id') is None:
      job_params = params
      task_metadata = {'task-id': None}
      task_params = {}
    else:
      job_params = {}
      task_metadata = {'task-id': str(job.get('task-id'))}
      task_params = params

    task_resources = Resources(logging_path=job.get('logging'))

    task_descriptors = [
        TaskDescriptor.get_complete_descriptor(task_metadata, task_params,
                                               task_resources)
    ]

    return JobDescriptor.get_complete_descriptor(
        job_metadata, job_params, job_resources, task_descriptors)

  @classmethod
  def from_yaml(cls, yaml_string):
    """Populate and return a JobDescriptor from a YAML string."""
    job = yaml.load(yaml_string)

    # If the YAML does not contain a top-level dsub version, then assume that
    # the string is coming from the local provider, reading an old version of
    # its meta.yaml.
    dsub_version = job.get('dsub-version')
    if not dsub_version:
      return cls._from_yaml_v0(job)

    job_metadata = {}
    for key in ['job-id', 'job-name', 'task-ids', 'user-id', 'dsub-version']:
      if job.get(key) is not None:
        job_metadata[key] = job.get(key)

    # Make sure that create-time string is turned into a datetime
    job_metadata['create-time'] = dsub_util.replace_timezone(
        job.get('create-time'), pytz.utc)

    job_resources = Resources(logging=job.get('logging'))

    job_params = {}
    job_params['labels'] = cls._label_params_from_dict(job.get('labels', {}))
    job_params['envs'] = cls._env_params_from_dict(job.get('envs', {}))
    job_params['inputs'] = cls._input_file_params_from_dict(
        job.get('inputs', {}), False) | cls._input_file_params_from_dict(
            job.get('input-recursives', {}), True)
    job_params['outputs'] = cls._output_file_params_from_dict(
        job.get('outputs', {}), False) | cls._output_file_params_from_dict(
            job.get('output-recursives', {}), True)

    task_descriptors = []
    for task in job.get('tasks', []):
      task_metadata = {'task-id': task.get('task-id')}

      task_params = {}
      task_params['labels'] = cls._label_params_from_dict(
          task.get('labels', {}))
      task_params['envs'] = cls._env_params_from_dict(task.get('envs', {}))
      task_params['inputs'] = cls._input_file_params_from_dict(
          task.get('inputs', {}), False) | cls._input_file_params_from_dict(
              task.get('input-recursives', {}), True)
      task_params['outputs'] = cls._output_file_params_from_dict(
          task.get('outputs', {}), False) | cls._output_file_params_from_dict(
              task.get('output-recursives', {}), True)

      task_resources = Resources(logging_path=task.get('logging-path'))

      task_descriptors.append(
          TaskDescriptor(task_metadata, task_params, task_resources))

    return JobDescriptor(job_metadata, job_params, job_resources,
                         task_descriptors)


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
