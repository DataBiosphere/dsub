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
"""Utility functions and classes for input, output, and script parameters."""

import collections
import csv
import os
import re

import dsub_util

AUTO_PREFIX_INPUT = 'INPUT_'  # Prefix for auto-generated input names
AUTO_PREFIX_OUTPUT = 'OUTPUT_'  # Prefix for auto-generated output names


def validate_param_name(name, param_type):
  """Validate that the name follows posix conventions for env variables."""
  # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_235
  #
  # 3.235 Name
  # In the shell command language, a word consisting solely of underscores,
  # digits, and alphabetics from the portable character set.
  if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
    raise ValueError('Invalid %s: %s' % (param_type, name))


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


class FileParam(
    collections.namedtuple('FileParam', [
        'name', 'value', 'docker_path', 'remote_uri', 'recursive'
    ])):
  """File parameter to be automatically localized or de-localized.

  Input files are automatically localized from GCS to the pipeline VM's
  local block disk(s).

  Output files are automatically de-localized to GCS from the pipeline VM's
  local block disk(s).

  Attributes:
    name (str): the parameter and environment variable name.
    value (str): the original value given by the user on the command line or
                 in the TSV file
    docker_path (str): the on-VM location; also set as the environment variable
                       value.
    remote_uri (str): the GCS path.
    recursive (bool): Whether recursive copy is wanted.
  """
  __slots__ = ()

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              remote_uri=None,
              recursive=False):
    return super(FileParam, cls).__new__(cls, name, value, docker_path,
                                         remote_uri, recursive)


class InputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              remote_uri=None,
              recursive=False):
    validate_param_name(name, 'Input parameter')
    return super(InputFileParam, cls).__new__(cls, name, value, docker_path,
                                              remote_uri, recursive)


class OutputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls,
              name,
              value=None,
              docker_path=None,
              remote_uri=None,
              recursive=False):
    validate_param_name(name, 'Output parameter')
    return super(OutputFileParam, cls).__new__(cls, name, value, docker_path,
                                               remote_uri, recursive)


class FileParamUtil(object):
  """Base class helper for producing FileParams from args or a tasks file.

  InputFileParams and OutputFileParams can be produced from either arguments
  passed on the command-line or as a combination of the definition in the tasks
  file header plus cell values in task records.

  This class encapsulates the generation of the FileParam name, if none is
  specified (get_variable_name()) as well as common path validation for
  input and output arguments (validate_paths).
  """

  def __init__(self, auto_prefix, relative_path):
    self._auto_prefix = auto_prefix
    self._auto_index = 0
    self._relative_path = relative_path

  def get_variable_name(self, name):
    """Produce a default variable name if none is specified."""
    if not name:
      name = '%s%s' % (self._auto_prefix, self._auto_index)
      self._auto_index += 1

    return name

  @staticmethod
  def _validate_paths(remote_uri):
    """Do basic validation of the remote_uri, return the path and filename."""

    # Only GCS paths are currently supported
    if not remote_uri.startswith('gs://'):
      raise ValueError('Only Cloud Storage URIs (gs://) supported: %s' %
                       remote_uri)

    # Only support file URLs and *filename* wildcards
    # Wildcards at the directory level or "**" syntax would require better
    # support from the Pipelines API *or* doing expansion here and
    # (potentially) producing a series of FileParams, instead of one.
    path = os.path.dirname(remote_uri)
    filename = os.path.basename(remote_uri)

    # dsub could support character ranges ([0-9]) with some more work, but for
    # now we assume that basic asterisk wildcards are sufficient. Reject any URI
    # that includes square brackets or question marks, since we know that
    # if they actually worked, it would be accidental.
    if '[' in remote_uri or ']' in remote_uri:
      raise ValueError('Square bracket (character ranges) are not supported: %s'
                       % remote_uri)

    if '?' in remote_uri:
      raise ValueError(
          'Question mark wildcards are not supported: %s' % remote_uri)

    if '*' in path:
      raise ValueError(
          'Wildcards in remote paths only supported for files: %s' % remote_uri)

    if '**' in filename:
      raise ValueError('Recursive wildcards ("**") not supported: %s' %
                       remote_uri)

    return path, filename

  @staticmethod
  def _uri_to_localpath(uri):
    return uri.replace('gs://', 'gs/')


class InputFileParamUtil(FileParamUtil):

  def __init__(self, docker_path):
    super(InputFileParamUtil, self).__init__(AUTO_PREFIX_INPUT, docker_path)

  def parse_uri(self, remote_uri, recursive):
    """Return a valid docker_path and remote_uri from the remote_uri."""

    # Validate and then tokenize the remote URI in order to build the
    # docker_path and remote_uri.
    path, filename = self._validate_paths(remote_uri)

    if recursive:
      # For recursive copies, the remote_uri must be a directory path, with
      # or without the trailing slash; the remote_uri cannot contain wildcards.
      if '*' in filename:
        raise ValueError(
            'Input variables that are recursive must not contain wildcards: %s'
            % remote_uri)

      # Non-recursive parameters explicitly set the target path to include
      # a trailing slash when the target path is a directory; be consistent
      # here and normalize the path to always have a single trailing slash
      remote_uri = remote_uri.rstrip('/') + '/'
      docker_path = '%s/%s' % (self._relative_path,
                               self._uri_to_localpath(remote_uri))
    else:
      # The translation for inputs of the remote_uri into the docker_path is
      # fairly straight forward.
      #
      # If the "filename" portion is a wildcard, then the docker_path must
      # explicitly be a directory, with a trailing slash, otherwise there can
      # be ambiguity based on the runtime inputs.
      #
      #   gsutil cp gs://bucket/path/*.bam <mnt>/input/gs/bucket/path
      #
      # produces different results depending on whether *.bam matches a single
      # file or multiple. In the first case, it produces a single file called
      # "path". In the second case it produces a directory called "path" with
      # multiple files.
      #
      # The result of:
      #
      #   gsutil cp gs://bucket/path/*.bam <mnt>/input/gs/bucket/path/
      #
      # is consistent: a directory called "path" with one or more files.
      docker_path = '%s/%s/' % (self._relative_path,
                                self._uri_to_localpath(path))

      # If the file portion of the path is not a wildcard, then the local target
      # is a filename.
      if '*' not in filename:
        docker_path += filename

    # The docker path is a relative path to the provider-specific mount point
    docker_path = docker_path.lstrip('/')

    return docker_path, remote_uri


class OutputFileParamUtil(FileParamUtil):

  def __init__(self, docker_path):
    super(OutputFileParamUtil, self).__init__(AUTO_PREFIX_OUTPUT, docker_path)

  def parse_uri(self, remote_uri, recursive):
    """Return a valid docker_path and remote_uri from the remote_uri."""

    # Validate and then tokenize the remote URI in order to build the
    # docker_path and remote_uri.
    #
    # For output variables, the "file" portion of the remote URI indicates
    # what to copy from the pipeline's docker path to the remote destination:
    #   gs://bucket/path/filename
    # turns into:
    #   gsutil cp <mnt>/output/gs/bucket/path/filename gs://bucket/path/
    #
    # In the above example, we would create:
    #   docker_path = output/gs/bucket/path/filename
    #   remote_uri = gs://bucket/path/
    path, filename = self._validate_paths(remote_uri)

    if recursive:
      # For recursive copies, the remote_uri must be a directory path, with
      # or without the trailing slash; the remote_uri cannot contain wildcards.
      if '*' in filename:
        raise ValueError(
            'Output variables that are recursive must not contain wildcards: %s'
            % remote_uri)

      # Non-recursive parameters explicitly set the target path to include
      # a trailing slash when the target path is a directory; be consistent
      # here and normalize the path to always have a single trailing slash
      remote_uri = remote_uri.rstrip('/') + '/'
      docker_path = '%s/%s' % (self._relative_path,
                               self._uri_to_localpath(remote_uri))
    else:
      # The remote_uri for a non-recursive output variable must be a filename
      # or a wildcard
      if remote_uri.endswith('/'):
        raise ValueError(
            'Output variables that are not recursive must reference a '
            'filename or wildcard: %s' % remote_uri)

      # Put the docker path together as:
      #  [output]/[gs/bucket/path/filename]
      docker_path = '%s/%s' % (self._relative_path,
                               self._uri_to_localpath(remote_uri))

      # If the filename contains a wildcard, make sure the remote_uri target
      # is clearly a directory path. Otherwise if the local wildcard ends up
      # matching a single file, the remote_uri will be a file, rather than a
      # directory containing a file.
      #
      # gsutil cp <mnt>/output/gs/bucket/path/*.bam gs://bucket/path
      #
      # produces different results if *.bam matches a single file vs. multiple.
      if '*' in filename:
        remote_uri = '%s/' % path

    return docker_path, remote_uri


def split_pair(pair_string, separator, nullable_idx=1):
  """Split a string into a pair, which can have one empty value.

  Args:
    pair_string: The string to be split.
    separator: The separator to be used for splitting.
    nullable_idx: The location to be set to null if the separator is not in the
                  input string. Should be either 0 or 1.

  Returns:
    A list containing the pair.

  Raises:
    IndexError: If nullable_idx is not 0 or 1.
  """

  pair = pair_string.split(separator, 1)
  if len(pair) == 1:
    if nullable_idx == 0:
      return [None, pair[0]]
    elif nullable_idx == 1:
      return [pair[0], None]
    else:
      raise IndexError('nullable_idx should be either 0 or 1.')
  else:
    return pair


def parse_tasks_file_header(header, input_file_param_util,
                            output_file_param_util):
  """Parse the header from the tasks file into env, input, output definitions.

  Elements are formatted similar to their equivalent command-line arguments,
  but with associated values coming from the data rows.

  Environment variables columns are headered as "--env <name>"
  Inputs columns are headered as "--input <name>" with the name optional.
  Outputs columns are headered as "--output <name>" with the name optional.

  For historical reasons, bareword column headers (such as "JOB_ID") are
  equivalent to "--env var_name".

  Args:
    header: Array of header fields
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.

  Returns:
    job_params: A list of EnvParams and FileParams for the environment
    variables, input file parameters, and output file parameters.

  Raises:
    ValueError: If a header contains a ":" and the prefix is not supported.
  """
  job_params = []

  for col in header:

    # Reserve the "-" and "--" namespace.
    # If the column has no leading "-", treat it as an environment variable
    col_type = '--env'
    col_value = col
    if col.startswith('-'):
      col_type, col_value = split_pair(col, ' ', 1)

    if col_type == '--env':
      job_params.append(EnvParam(col_value))

    elif col_type == '--input' or col_type == '--input-recursive':
      name = input_file_param_util.get_variable_name(col_value)
      job_params.append(
          InputFileParam(name, recursive=(col_type.endswith('recursive'))))

    elif col_type == '--output' or col_type == '--output-recursive':
      name = output_file_param_util.get_variable_name(col_value)
      job_params.append(
          OutputFileParam(name, recursive=(col_type.endswith('recursive'))))

    else:
      raise ValueError('Unrecognized column header: %s' % col)

  return job_params


def tasks_file_to_job_data(tasks, input_file_param_util,
                           output_file_param_util):
  """Parses task parameters from a TSV.

  Args:
    tasks: Dict containing the path to a TSV file and task numbers to run
    variables, input, and output parameters as column headings. Subsequent
    lines specify parameter values, one row per job.
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.

  Returns:
    job_data: an array of records, each containing a dictionary of
    'envs', 'inputs', and 'outputs' that defines the set of parameters and data
    for each job.

  Raises:
    ValueError: If no job records were provided
  """
  job_data = []

  path = tasks['path']
  task_min = tasks.get('min')
  task_max = tasks.get('max')

  # Load the file and set up a Reader that tokenizes the fields
  param_file = dsub_util.load_file(path)
  reader = csv.reader(param_file, delimiter='\t')

  # Read the first line and extract the parameters
  header = reader.next()
  job_params = parse_tasks_file_header(header, input_file_param_util,
                                       output_file_param_util)

  # Build a list of records from the parsed input file
  for row in reader:
    # Tasks are numbered starting at 1 and since the first line of the TSV
    # file is a header, the first task appears on line 2.
    task_id = reader.line_num - 1
    if task_min and task_id < task_min:
      continue
    if task_max and task_id > task_max:
      continue

    if len(row) != len(job_params):
      dsub_util.print_error('Unexpected number of fields %s vs %s: line %s' %
                            (len(row), len(job_params), reader.line_num))

    # Each row can contain "envs", "inputs", "outputs"
    envs = []
    inputs = []
    outputs = []

    for i in range(0, len(job_params)):
      param = job_params[i]
      if isinstance(param, EnvParam):
        envs.append(EnvParam(param.name, row[i]))

      elif isinstance(param, InputFileParam):
        docker_path, remote_uri = input_file_param_util.parse_uri(
            row[i], param.recursive)
        inputs.append(
            InputFileParam(param.name, row[i], docker_path, remote_uri,
                           param.recursive))

      elif isinstance(param, OutputFileParam):
        docker_path, remote_uri = output_file_param_util.parse_uri(
            row[i], param.recursive)
        outputs.append(
            OutputFileParam(param.name, row[i], docker_path, remote_uri,
                            param.recursive))

    job_data.append({
        'task_id': task_id,
        'envs': envs,
        'inputs': inputs,
        'outputs': outputs
    })

  # Ensure that there are jobs to execute (and not just a header)
  if not job_data:
    raise ValueError('No tasks added from %s' % path)

  return job_data


def args_to_job_data(envs, inputs, inputs_recursive, outputs, outputs_recursive,
                     input_file_param_util, output_file_param_util):
  """Parse env, input, and output parameters into a job parameters and data.

  Passing arguments on the command-line allows for launching a single job.
  The env, input, and output arguments encode both the definition of the
  job as well as the single job's values.

  Env arguments are simple name=value pairs.
  Input and output file arguments can contain name=value pairs or just values.
  Either of the following is valid:

    remote_uri
    myfile=remote_uri

  Args:
    envs: list of environment variable job parameters
    inputs: list of file input parameters
    inputs_recursive: list of recursive directory input parameters
    outputs: list of file output parameters
    outputs_recursive: list of recursive directory output parameters
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.

  Returns:
    job_data: an array of length one, containing a dictionary of
    'envs', 'inputs', and 'outputs' that defines the set of parameters and data
    for a job.
  """

  # For environment variables, we need to:
  #   * split the input into name=value pairs (value optional)
  #   * Create the EnvParam object
  env_data = []
  for arg in envs:
    name, value = split_pair(arg, '=', nullable_idx=1)
    env_data.append(EnvParam(name, value))

  # For input files, we need to:
  #   * split the input into name=remote_uri pairs (name optional)
  #   * validate the remote uri
  #   * generate a docker_path
  input_data = []
  for (recursive, args) in ((False, inputs), (True, inputs_recursive)):
    for arg in args:
      name, value = split_pair(arg, '=', nullable_idx=0)
      name = input_file_param_util.get_variable_name(name)
      docker_path, remote_uri = input_file_param_util.parse_uri(
          value, recursive)
      input_data.append(
          InputFileParam(name, value, docker_path, remote_uri, recursive))

  # For output files, we need to:
  #   * split the input into name=remote_uri pairs (name optional)
  #   * validate the remote uri
  #   * generate the remote uri
  #   * generate a docker_path
  output_data = []
  for (recursive, args) in ((False, outputs), (True, outputs_recursive)):
    for arg in args:
      name, value = split_pair(arg, '=', 0)

      name = output_file_param_util.get_variable_name(name)
      docker_path, remote_uri = output_file_param_util.parse_uri(
          value, recursive)
      output_data.append(
          OutputFileParam(name, value, docker_path, remote_uri, recursive))

  return [{
      'envs': env_data,
      'inputs': input_data,
      'outputs': output_data,
  }]
