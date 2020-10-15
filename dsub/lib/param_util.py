# Lint as: python3
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
"""Utility functions and classes for dsub command-line parameters."""

import argparse
import csv
import datetime
import os
import re
import sys
from . import dsub_util
from . import job_model
from .._dsub_version import DSUB_VERSION
from dateutil.tz import tzlocal
import pytz

AUTO_PREFIX_INPUT = 'INPUT_'  # Prefix for auto-generated input names
AUTO_PREFIX_OUTPUT = 'OUTPUT_'  # Prefix for auto-generated output names


class ListParamAction(argparse.Action):
  """Append each value as a separate element to the parser destination.

  This class satisifes the action interface of argparse.ArgumentParser and
  refines the 'append' action for arguments with `nargs='*'`.

  For the parameters:

    --myarg val1 val2 --myarg val3

  The 'append' action yields:

    args.myval = ['val1 val2', 'val3']

  While ListParamAction yields:

    args.myval = ['val1', 'val2', 'val3']
  """

  def __init__(self, option_strings, dest, **kwargs):
    super(ListParamAction, self).__init__(option_strings, dest, **kwargs)

  def __call__(self, parser, namespace, values, option_string=None):
    params = getattr(namespace, self.dest, [])

    # Input comes in as a list (possibly len=1) of NAME=VALUE pairs
    for arg in values:
      params.append(arg)
    setattr(namespace, self.dest, params)


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
    self.param_class = job_model.FileParam
    self._auto_prefix = auto_prefix
    self._auto_index = 0
    self._relative_path = relative_path

  def get_variable_name(self, name):
    """Produce a default variable name if none is specified."""
    if not name:
      name = '%s%s' % (self._auto_prefix, self._auto_index)
      self._auto_index += 1
    return name

  def rewrite_uris(self, raw_uri, file_provider):
    """Accept a raw uri and return rewritten versions.

    This function returns a normalized URI and a docker path. The normalized
    URI may have minor alterations meant to disambiguate and prepare for use
    by shell utilities that may require a specific format.

    The docker rewriter makes substantial modifications to the raw URI when
    constructing a docker path, but modifications must follow these rules:
      1) System specific characters are not allowed (ex. indirect paths).
      2) The path, if it is a directory, must end in a forward slash.
      3) The path will begin with the value set in self._relative_path.
      4) The path will have an additional prefix (after self._relative_path) set
         by the file provider-specific rewriter.

    Rewrite output for the docker path:
      >>> out_util = FileParamUtil('AUTO_', 'output')
      >>> out_util.rewrite_uris('gs://mybucket/myfile.txt', job_model.P_GCS)[1]
      'output/gs/mybucket/myfile.txt'
      >>> out_util.rewrite_uris('./data/myfolder/', job_model.P_LOCAL)[1]
      'output/file/data/myfolder/'

    When normalizing the URI for cloud buckets, no rewrites are done. For local
    files, the user directory will be expanded and relative paths will be
    converted to absolute:
      >>> in_util = FileParamUtil('AUTO_', 'input')
      >>> in_util.rewrite_uris('gs://mybucket/gcs_dir/', job_model.P_GCS)[0]
      'gs://mybucket/gcs_dir/'
      >>> in_util.rewrite_uris('/data/./dir_a/../myfile.txt',
      ...   job_model.P_LOCAL)[0]
      '/data/myfile.txt'
      >>> in_util.rewrite_uris('file:///tmp/data/*.bam', job_model.P_LOCAL)[0]
      '/tmp/data/*.bam'

    Args:
      raw_uri: (str) the path component of the raw URI.
      file_provider: a valid provider (contained in job_model.FILE_PROVIDERS).

    Returns:
      normalized: a cleaned version of the uri provided by command line.
      docker_path: the uri rewritten in the format required for mounting inside
                   a docker worker.

    Raises:
      ValueError: if file_provider is not valid.
    """
    if file_provider == job_model.P_GCS:
      normalized, docker_path = _gcs_uri_rewriter(raw_uri)
    elif file_provider == job_model.P_LOCAL:
      normalized, docker_path = _local_uri_rewriter(raw_uri)
    else:
      raise ValueError('File provider not supported: %r' % file_provider)
    return normalized, os.path.join(self._relative_path, docker_path)

  @staticmethod
  def parse_file_provider(uri):
    """Find the file provider for a URI."""
    providers = {'gs': job_model.P_GCS, 'file': job_model.P_LOCAL}
    # URI scheme detector uses a range up to 30 since none of the IANA
    # registered schemes are longer than this.
    provider_found = re.match(r'^([A-Za-z][A-Za-z0-9+.-]{0,29})://', uri)
    if provider_found:
      prefix = provider_found.group(1).lower()
    else:
      # If no provider is specified in the URI, assume that the local
      # filesystem is being used. Availability and validity of the local
      # file/directory will be checked later.
      prefix = 'file'
    if prefix in providers:
      return providers[prefix]
    else:
      raise ValueError('File prefix not supported: %s://' % prefix)

  @staticmethod
  def _validate_paths_or_fail(uri, recursive):
    """Do basic validation of the uri, return the path and filename."""
    path, filename = os.path.split(uri)

    # dsub could support character ranges ([0-9]) with some more work, but for
    # now we assume that basic asterisk wildcards are sufficient. Reject any URI
    # that includes square brackets or question marks, since we know that
    # if they actually worked, it would be accidental.
    if '[' in uri or ']' in uri:
      raise ValueError(
          'Square bracket (character ranges) are not supported: %s' % uri)
    if '?' in uri:
      raise ValueError('Question mark wildcards are not supported: %s' % uri)

    # Only support file URIs and *filename* wildcards
    # Wildcards at the directory level or "**" syntax would require better
    # support from the Pipelines API *or* doing expansion here and
    # (potentially) producing a series of FileParams, instead of one.
    if '*' in path:
      raise ValueError(
          'Path wildcard (*) are only supported for files: %s' % uri)
    if '**' in filename:
      raise ValueError('Recursive wildcards ("**") not supported: %s' % uri)
    if filename in ('..', '.'):
      raise ValueError('Path characters ".." and "." not supported '
                       'for file names: %s' % uri)

    # Do not allow non-recursive IO to reference directories.
    if not recursive and not filename:
      raise ValueError('Input or output values that are not recursive must '
                       'reference a filename or wildcard: %s' % uri)

  def parse_uri(self, raw_uri, recursive):
    """Return a valid docker_path, uri, and file provider from a flag value."""
    # Assume recursive URIs are directory paths.
    if recursive:
      raw_uri = directory_fmt(raw_uri)
    # Get the file provider, validate the raw URI, and rewrite the path
    # component of the URI for docker and remote.
    file_provider = self.parse_file_provider(raw_uri)
    self._validate_paths_or_fail(raw_uri, recursive)
    uri, docker_uri = self.rewrite_uris(raw_uri, file_provider)
    uri_parts = job_model.UriParts(
        directory_fmt(os.path.dirname(uri)), os.path.basename(uri))
    return docker_uri, uri_parts, file_provider

  def make_param(self, name, raw_uri, recursive):
    """Return a *FileParam given an input uri."""
    if not raw_uri:
      return self.param_class(name, None, None, None, recursive, None)
    docker_path, uri_parts, provider = self.parse_uri(raw_uri, recursive)
    return self.param_class(name, raw_uri, docker_path, uri_parts, recursive,
                            provider)


class InputFileParamUtil(FileParamUtil):
  """Implementation of FileParamUtil for input files."""

  def __init__(self, docker_path):
    super(InputFileParamUtil, self).__init__(AUTO_PREFIX_INPUT, docker_path)
    self.param_class = job_model.InputFileParam


class OutputFileParamUtil(FileParamUtil):
  """Implementation of FileParamUtil for output files."""

  def __init__(self, docker_path):
    super(OutputFileParamUtil, self).__init__(AUTO_PREFIX_OUTPUT, docker_path)
    self.param_class = job_model.OutputFileParam


class MountParamUtil(object):
  """Utility class for --mount parameter."""

  def __init__(self, docker_path):
    self._relative_path = docker_path

  def _parse_image_uri(self, raw_uri):
    """Return a valid docker_path from a Google Persistent Disk url."""
    # The string replace is so we don't have colons and double slashes in the
    # mount path. The idea is the resulting mount path would look like:
    # /mnt/data/mount/http/www.googleapis.com/compute/v1/projects/...
    docker_uri = os.path.join(self._relative_path,
                              raw_uri.replace('https://', 'https/', 1))
    return docker_uri

  def _parse_local_mount_uri(self, raw_uri):
    """Return a valid docker_path for a local file path."""
    raw_uri = directory_fmt(raw_uri)
    _, docker_path = _local_uri_rewriter(raw_uri)
    local_path = docker_path[len('file'):]
    docker_uri = os.path.join(self._relative_path, docker_path)
    return local_path, docker_uri

  def _parse_gcs_uri(self, raw_uri):
    """Return a valid docker_path for a GCS bucket."""
    # Assume URI is a directory path.
    raw_uri = directory_fmt(raw_uri)
    _, docker_path = _gcs_uri_rewriter(raw_uri)
    docker_uri = os.path.join(self._relative_path, docker_path)
    return docker_uri

  def make_param(self, name, raw_uri, disk_size):
    """Return a MountParam given a GCS bucket, disk image or local path."""
    if raw_uri.startswith('https://www.googleapis.com/compute'):
      # Full Image URI should look something like:
      # https://www.googleapis.com/compute/v1/projects/<project>/global/images/
      # But don't validate further, should the form of a valid image URI
      # change (v1->v2, for example)
      docker_path = self._parse_image_uri(raw_uri)
      return job_model.PersistentDiskMountParam(
          name, raw_uri, docker_path, disk_size, disk_type=None)
    elif raw_uri.startswith('file://'):
      local_path, docker_path = self._parse_local_mount_uri(raw_uri)
      return job_model.LocalMountParam(name, raw_uri, docker_path, local_path)
    elif raw_uri.startswith('gs://'):
      docker_path = self._parse_gcs_uri(raw_uri)
      return job_model.GCSMountParam(name, raw_uri, docker_path)
    else:
      raise ValueError(
          'Mount parameter {} must begin with valid prefix.'.format(raw_uri))


def _gcs_uri_rewriter(raw_uri):
  """Rewrite GCS file paths as required by the rewrite_uris method.

  The GCS rewriter performs no operations on the raw_path and simply returns
  it as the normalized URI. The docker path has the gs:// prefix replaced
  with gs/ so that it can be mounted inside a docker image.

  Args:
    raw_uri: (str) the raw GCS URI, prefix, or pattern.

  Returns:
    normalized: a cleaned version of the uri provided by command line.
    docker_path: the uri rewritten in the format required for mounting inside
                 a docker worker.
  """
  docker_path = raw_uri.replace('gs://', 'gs/', 1)
  return raw_uri, docker_path


def _local_uri_rewriter(raw_uri):
  """Rewrite local file URIs as required by the rewrite_uris method.

  Local file paths, unlike GCS paths, may have their raw URI simplified by
  os.path.normpath which collapses extraneous indirect characters.

  >>> _local_uri_rewriter('/tmp/a_path/../B_PATH/file.txt')
  ('/tmp/B_PATH/file.txt', 'file/tmp/B_PATH/file.txt')
  >>> _local_uri_rewriter('/myhome/./mydir/')
  ('/myhome/mydir/', 'file/myhome/mydir/')

  The local path rewriter will also work to preserve relative paths even
  when creating the docker path. This prevents leaking of information on the
  invoker's system to the remote system. Doing this requires a number of path
  substitutions denoted with the _<rewrite>_ convention.

  >>> _local_uri_rewriter('./../upper_dir/')[1]
  'file/_dotdot_/upper_dir/'
  >>> _local_uri_rewriter('~/localdata/*.bam')[1]
  'file/_home_/localdata/*.bam'

  Args:
    raw_uri: (str) the raw file or directory path.

  Returns:
    normalized: a simplified and/or expanded version of the uri.
    docker_path: the uri rewritten in the format required for mounting inside
                 a docker worker.

  """
  # The path is split into components so that the filename is not rewritten.
  raw_path, filename = os.path.split(raw_uri)
  # Generate the local path that can be resolved by filesystem operations,
  # this removes special shell characters, condenses indirects and replaces
  # any unnecessary prefix.
  prefix_replacements = [('file:///', '/'), ('~/', os.getenv('HOME')), ('./',
                                                                        ''),
                         ('file:/', '/')]
  normed_path = raw_path
  for prefix, replacement in prefix_replacements:
    if normed_path.startswith(prefix):
      normed_path = os.path.join(replacement, normed_path[len(prefix):])
  # Because abspath strips the trailing '/' from bare directory references
  # other than root, this ensures that all directory references end with '/'.
  normed_uri = directory_fmt(os.path.abspath(normed_path))
  normed_uri = os.path.join(normed_uri, filename)

  # Generate the path used inside the docker image;
  #  1) Get rid of extra indirects: /this/./that -> /this/that
  #  2) Rewrite required indirects as synthetic characters.
  #  3) Strip relative or absolute path leading character.
  #  4) Add 'file/' prefix.
  docker_rewrites = [(r'/\.\.', '/_dotdot_'), (r'^\.\.', '_dotdot_'),
                     (r'^~/', '_home_/'), (r'^file:/', '')]
  docker_path = os.path.normpath(raw_path)
  for pattern, replacement in docker_rewrites:
    docker_path = re.sub(pattern, replacement, docker_path)
  docker_path = docker_path.lstrip('./')  # Strips any of '.' './' '/'.
  docker_path = directory_fmt('file/' + docker_path) + filename
  return normed_uri, docker_path


def get_gcs_mounts(mounts):
  """Returns the GCS mounts from mounts."""
  return _get_filtered_mounts(mounts, job_model.GCSMountParam)


def get_persistent_disk_mounts(mounts):
  """Returns the persistent disk mounts from mounts."""
  return _get_filtered_mounts(mounts, job_model.PersistentDiskMountParam)


def get_local_mounts(mounts):
  """Returns the local mounts from mounts."""
  return _get_filtered_mounts(mounts, job_model.LocalMountParam)


def _get_filtered_mounts(mounts, mount_param_type):
  """Helper function to return an appropriate set of mount parameters."""
  return set([mount for mount in mounts if isinstance(mount, mount_param_type)])


def build_logging_param(logging_uri, util_class=OutputFileParamUtil):
  """Convenience function simplifies construction of the logging uri."""
  if not logging_uri:
    return job_model.LoggingParam(None, None)
  recursive = not logging_uri.endswith('.log')
  oututil = util_class('')
  _, uri, provider = oututil.parse_uri(logging_uri, recursive)
  if '*' in uri.basename:
    raise ValueError('Wildcards not allowed in logging URI: %s' % uri)
  return job_model.LoggingParam(uri, provider)


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
    variables, LabelParams, input file parameters, and output file parameters.

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
      job_params.append(job_model.EnvParam(col_value))

    elif col_type == '--label':
      job_params.append(job_model.LabelParam(col_value))

    elif col_type == '--input' or col_type == '--input-recursive':
      name = input_file_param_util.get_variable_name(col_value)
      job_params.append(
          job_model.InputFileParam(
              name, recursive=(col_type.endswith('recursive'))))

    elif col_type == '--output' or col_type == '--output-recursive':
      name = output_file_param_util.get_variable_name(col_value)
      job_params.append(
          job_model.OutputFileParam(
              name, recursive=(col_type.endswith('recursive'))))

    else:
      raise ValueError('Unrecognized column header: %s' % col)

  return job_params


def tasks_file_to_task_descriptors(tasks, retries, input_file_param_util,
                                   output_file_param_util):
  """Parses task parameters from a TSV.

  Args:
    tasks: Dict containing the path to a TSV file and task numbers to run
    variables, input, and output parameters as column headings. Subsequent
    lines specify parameter values, one row per job.
    retries: Number of retries allowed.
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.

  Returns:
    task_descriptors: an array of records, each containing the task-id,
    task-attempt, 'envs', 'inputs', 'outputs', 'labels' that defines the set of
    parameters for each task of the job.

  Raises:
    ValueError: If no job records were provided
  """
  task_descriptors = []

  path = tasks['path']
  task_min = tasks.get('min')
  task_max = tasks.get('max')

  # First check for any empty lines
  param_file = dsub_util.load_file(path)
  param_file_lines = param_file.splitlines()
  if any([not line for line in param_file_lines]):
    raise ValueError('Blank line(s) found in {}'.format(path))

  # Set up a Reader that tokenizes the fields
  reader = csv.reader(param_file_lines, delimiter='\t')

  # Read the first line and extract the parameters
  header = next(reader)
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
      raise ValueError(
          'Unexpected number of fields {} vs {}: in {} line {}'.format(
              len(row), len(job_params), path, reader.line_num))

    # Each row can contain "envs", "inputs", "outputs"
    envs = set()
    inputs = set()
    outputs = set()
    labels = set()

    for i in range(0, len(job_params)):
      param = job_params[i]
      name = param.name
      if isinstance(param, job_model.EnvParam):
        envs.add(job_model.EnvParam(name, row[i]))

      elif isinstance(param, job_model.LabelParam):
        labels.add(job_model.LabelParam(name, row[i]))

      elif isinstance(param, job_model.InputFileParam):
        inputs.add(
            input_file_param_util.make_param(name, row[i], param.recursive))

      elif isinstance(param, job_model.OutputFileParam):
        outputs.add(
            output_file_param_util.make_param(name, row[i], param.recursive))

    task_descriptors.append(
        job_model.TaskDescriptor({
            'task-id': task_id,
            'task-attempt': 1 if retries else None
        }, {
            'labels': labels,
            'envs': envs,
            'inputs': inputs,
            'outputs': outputs
        }, job_model.Resources()))

  # Ensure that there are jobs to execute (and not just a header)
  if not task_descriptors:
    raise ValueError('No tasks added from %s' % path)

  return task_descriptors


def parse_pair_args(labels, argclass):
  """Parse flags of key=value pairs and return a list of argclass.

  For pair variables, we need to:
     * split the input into name=value pairs (value optional)
     * Create the EnvParam object

  Args:
    labels: list of 'key' or 'key=value' strings.
    argclass: Container class for args, must instantiate with argclass(k, v).

  Returns:
    list of argclass objects.
  """
  label_data = set()
  for arg in labels:
    name, value = split_pair(arg, '=', nullable_idx=1)
    label_data.add(argclass(name, value))
  return label_data


def args_to_job_params(envs, labels, inputs, inputs_recursive, outputs,
                       outputs_recursive, mounts, input_file_param_util,
                       output_file_param_util, mount_param_util):
  """Parse env, input, and output parameters into a job parameters and data.

  Passing arguments on the command-line allows for launching a single job.
  The env, input, and output arguments encode both the definition of the
  job as well as the single job's values.

  Env arguments are simple name=value pairs.
  Input and output file arguments can contain name=value pairs or just values.
  Either of the following is valid:

    uri
    myfile=uri

  Args:
    envs: list of environment variable job parameters
    labels: list of labels to attach to the tasks
    inputs: list of file input parameters
    inputs_recursive: list of recursive directory input parameters
    outputs: list of file output parameters
    outputs_recursive: list of recursive directory output parameters
    mounts: list of gcs buckets to mount
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.
    mount_param_util: Utility for producing MountParam objects.

  Returns:
    job_params: a dictionary of 'envs', 'inputs', and 'outputs' that defines the
    set of parameters and data for a job.
  """
  # Parse environmental variables and labels.
  env_data = parse_pair_args(envs, job_model.EnvParam)
  label_data = parse_pair_args(labels, job_model.LabelParam)

  # For input files, we need to:
  #   * split the input into name=uri pairs (name optional)
  #   * get the environmental variable name, or automatically set if null.
  #   * create the input file param
  input_data = set()
  for (recursive, args) in ((False, inputs), (True, inputs_recursive)):
    for arg in args:
      name, value = split_pair(arg, '=', nullable_idx=0)
      name = input_file_param_util.get_variable_name(name)
      input_data.add(input_file_param_util.make_param(name, value, recursive))

  # For output files, we need to:
  #   * split the input into name=uri pairs (name optional)
  #   * get the environmental variable name, or automatically set if null.
  #   * create the output file param
  output_data = set()
  for (recursive, args) in ((False, outputs), (True, outputs_recursive)):
    for arg in args:
      name, value = split_pair(arg, '=', 0)
      name = output_file_param_util.get_variable_name(name)
      output_data.add(output_file_param_util.make_param(name, value, recursive))

  mount_data = set()
  for arg in mounts:
    # Mounts can look like `--mount VAR=PATH` or `--mount VAR=PATH {num}`,
    # where num is the size of the disk in Gb. We assume a space is the
    # separator between path and disk size.
    if ' ' in arg:
      key_value_pair, disk_size = arg.split(' ')
      name, value = split_pair(key_value_pair, '=', 1)
      mount_data.add(mount_param_util.make_param(name, value, disk_size))
    else:
      name, value = split_pair(arg, '=', 1)
      mount_data.add(mount_param_util.make_param(name, value, disk_size=None))
  return {
      'envs': env_data,
      'inputs': input_data,
      'outputs': output_data,
      'labels': label_data,
      'mounts': mount_data,
  }


def _validate_providers(fileparams, argname, providers, provider_name):
  error_message = ('Unsupported {argname} path ({path}) for '
                   'provider {provider!r}.')
  for fileparam in fileparams:
    if not fileparam.uri:
      continue
    if fileparam.file_provider not in providers:
      raise ValueError(
          error_message.format(
              argname=argname, path=fileparam.uri, provider=provider_name))


def validate_submit_args_or_fail(job_descriptor, provider_name, input_providers,
                                 output_providers, logging_providers):
  """Validate that arguments passed to submit_job have valid file providers.

  This utility function takes resources and task data args from `submit_job`
  in the base provider. This function will fail with a value error if any of the
  parameters are not valid. See the following example;

  >>> job_resources = type('', (object,),
  ...    {"logging": job_model.LoggingParam('gs://logtemp', job_model.P_GCS)})()
  >>> job_params={'inputs': set(), 'outputs': set(), 'mounts': set()}
  >>> task_descriptors = [
  ...     job_model.TaskDescriptor(None, {
  ...       'inputs': {
  ...           job_model.FileParam('IN', uri='gs://in/*',
  ...                               file_provider=job_model.P_GCS)},
  ...       'outputs': set()}, None),
  ...     job_model.TaskDescriptor(None, {
  ...       'inputs': set(),
  ...       'outputs': {
  ...           job_model.FileParam('OUT', uri='gs://out/*',
  ...                               file_provider=job_model.P_GCS)}}, None)]
  ...
  >>> validate_submit_args_or_fail(job_model.JobDescriptor(None, job_params,
  ...                              job_resources, task_descriptors),
  ...                              provider_name='MYPROVIDER',
  ...                              input_providers=[job_model.P_GCS],
  ...                              output_providers=[job_model.P_GCS],
  ...                              logging_providers=[job_model.P_GCS])
  ...
  >>> validate_submit_args_or_fail(job_model.JobDescriptor(None, job_params,
  ...                              job_resources, task_descriptors),
  ...                              provider_name='MYPROVIDER',
  ...                              input_providers=[job_model.P_GCS],
  ...                              output_providers=[job_model.P_LOCAL],
  ...                              logging_providers=[job_model.P_GCS])
  Traceback (most recent call last):
       ...
  ValueError: Unsupported output path (gs://out/*) for provider 'MYPROVIDER'.

  Args:
    job_descriptor: instance of job_model.JobDescriptor.
    provider_name: (str) the name of the execution provider.
    input_providers: (string collection) whitelist of file providers for input.
    output_providers: (string collection) whitelist of providers for output.
    logging_providers: (string collection) whitelist of providers for logging.

  Raises:
    ValueError: if any file providers do not match the whitelists.
  """
  job_resources = job_descriptor.job_resources
  job_params = job_descriptor.job_params
  task_descriptors = job_descriptor.task_descriptors

  # Validate logging file provider.
  _validate_providers([job_resources.logging], 'logging', logging_providers,
                      provider_name)

  # Validate job input and output file providers
  _validate_providers(job_params['inputs'], 'input', input_providers,
                      provider_name)
  _validate_providers(job_params['outputs'], 'output', output_providers,
                      provider_name)

  # Validate input and output file providers.
  for task_descriptor in task_descriptors:
    _validate_providers(task_descriptor.task_params['inputs'], 'input',
                        input_providers, provider_name)
    _validate_providers(task_descriptor.task_params['outputs'], 'output',
                        output_providers, provider_name)


def directory_fmt(directory):
  """In ensure that directories end with '/'.

  Frequently we need to ensure that directory paths end with a forward slash.
  Pythons dirname and split functions in the path library treat this
  inconsistently creating this requirement. This function is simple but was
  written to centralize documentation of an often used (and often explained)
  requirement in this codebase.

  >>> os.path.dirname('gs://bucket/folder/file.txt')
  'gs://bucket/folder'
  >>> directory_fmt(os.path.dirname('gs://bucket/folder/file.txt'))
  'gs://bucket/folder/'
  >>> os.path.dirname('/newfile')
  '/'
  >>> directory_fmt(os.path.dirname('/newfile'))
  '/'

  Specifically we need this since copy commands must know whether the
  destination is a directory to function properly. See the following shell
  interaction for an example of the inconsistency. Notice that text files are
  copied as expected but the bam is copied over the directory name.

  Multiple files copy, works as intended in all cases:
      $ touch a.txt b.txt
      $ gsutil cp ./*.txt gs://mybucket/text_dest
      $ gsutil ls gs://mybucket/text_dest/
            0  2017-07-19T21:44:36Z  gs://mybucket/text_dest/a.txt
            0  2017-07-19T21:44:36Z  gs://mybucket/text_dest/b.txt
      TOTAL: 2 objects, 0 bytes (0 B)

  Single file copy fails to copy into a directory:
      $ touch 1.bam
      $ gsutil cp ./*.bam gs://mybucket/bad_dest
      $ gsutil ls gs://mybucket/bad_dest
               0  2017-07-19T21:46:16Z  gs://mybucket/bad_dest
      TOTAL: 1 objects, 0 bytes (0 B)

  Adding a trailing forward slash fixes this:
      $ touch my.sam
      $ gsutil cp ./*.sam gs://mybucket/good_folder
      $ gsutil ls gs://mybucket/good_folder
               0  2017-07-19T21:46:16Z  gs://mybucket/good_folder/my.sam
      TOTAL: 1 objects, 0 bytes (0 B)

  Args:
    directory (str): a uri without an blob or file basename.

  Returns:
    the directory with a trailing slash.
  """
  return directory.rstrip('/') + '/'


def handle_version_flag():
  """If the --version flag is passed, print version to stdout and exit.

  Within dsub commands, --version should be the highest priority flag.
  This function supplies a repeatable and DRY way of checking for the
  version flag and printing the version. Callers still need to define a version
  flag in the command's flags so that it shows up in help output.
  """
  parser = argparse.ArgumentParser(description='Version parser', add_help=False)
  parser.add_argument('--version', '-v', dest='version', action='store_true')
  parser.set_defaults(version=False)
  args, _ = parser.parse_known_args()
  if args.version:
    print('dsub version: %s' % DSUB_VERSION)
    sys.exit()


def age_to_create_time(age, from_time=None):
  """Compute the create time (UTC) for the list filter.

  If the age is an integer value it is treated as a UTC date.
  Otherwise the value must be of the form "<integer><unit>" where supported
  units are s, m, h, d, w (seconds, minutes, hours, days, weeks).

  Args:
    age: A "<integer><unit>" string or integer value.
    from_time:

  Returns:
    A timezone-aware datetime or None if age parameter is empty.
  """

  if not age:
    return None

  if not from_time:
    from_time = dsub_util.replace_timezone(datetime.datetime.now(), tzlocal())

  try:
    last_char = age[-1]

    if last_char == 's':
      return from_time - datetime.timedelta(seconds=int(age[:-1]))
    elif last_char == 'm':
      return from_time - datetime.timedelta(minutes=int(age[:-1]))
    elif last_char == 'h':
      return from_time - datetime.timedelta(hours=int(age[:-1]))
    elif last_char == 'd':
      return from_time - datetime.timedelta(days=int(age[:-1]))
    elif last_char == 'w':
      return from_time - datetime.timedelta(weeks=int(age[:-1]))
    else:
      # If no unit is given treat the age as seconds from epoch, otherwise apply
      # the correct time unit.
      return dsub_util.replace_timezone(
          datetime.datetime.utcfromtimestamp(int(age)), pytz.utc)

  except (ValueError, OverflowError) as e:
    raise ValueError('Unable to parse age string %s: %s' % (age, e))


def _interval_to_seconds(interval, valid_units='smhdw'):
  """Convert the timeout duration to seconds.

  The value must be of the form "<integer><unit>" where supported
  units are s, m, h, d, w (seconds, minutes, hours, days, weeks).

  Args:
    interval: A "<integer><unit>" string.
    valid_units: A list of supported units.

  Returns:
    A string of the form "<integer>s" or None if timeout is empty.
  """
  if not interval:
    return None

  try:
    last_char = interval[-1]

    if last_char == 's' and 's' in valid_units:
      return str(float(interval[:-1])) + 's'
    elif last_char == 'm' and 'm' in valid_units:
      return str(float(interval[:-1]) * 60) + 's'
    elif last_char == 'h' and 'h' in valid_units:
      return str(float(interval[:-1]) * 60 * 60) + 's'
    elif last_char == 'd' and 'd' in valid_units:
      return str(float(interval[:-1]) * 60 * 60 * 24) + 's'
    elif last_char == 'w' and 'w' in valid_units:
      return str(float(interval[:-1]) * 60 * 60 * 24 * 7) + 's'
    else:
      raise ValueError(
          'Unsupported units in interval string %s: %s' % (interval, last_char))

  except (ValueError, OverflowError) as e:
    raise ValueError('Unable to parse interval string %s: %s' % (interval, e))


def timeout_in_seconds(timeout):
  return _interval_to_seconds(timeout, valid_units='smhdw')


def log_interval_in_seconds(log_interval):
  return _interval_to_seconds(log_interval, valid_units='smh')


class PreemptibleParam(object):
  """Utility class for user specified --preemptible argument.

  The --preemptible arg can be set to one of three 'modes':
  1) Not given. Never run on a preemptible VM. Internally stored as 'False'.
  2) Given. Always run on a preemptible VM. Internally stored as 'True'.
  3) Given, and passed an integer p. Run on a preemptible VM up to p times
    before falling back to a full-price VM. Internally stored as an integer.
  """

  def __init__(self, p):
    self._max_preemptible_attempts = p

  def should_use_preemptible(self, attempt_number):
    if bool is type(self._max_preemptible_attempts):
      return self._max_preemptible_attempts
    else:
      return self._max_preemptible_attempts >= attempt_number

  def validate(self, retries):
    """Validates that preemptible arguments make sense with retries."""
    if int is type(self._max_preemptible_attempts):
      if retries < 0 or self._max_preemptible_attempts < 0:
        raise ValueError('--retries and --preemptible may not be negative')

      if self._max_preemptible_attempts >= 1 and not retries:
        # This means user specified a positive preemptible number
        # but didn't specify a retries number
        raise ValueError(
            'Requesting 1 or more preemptible attempts requires setting retries'
        )

      if self._max_preemptible_attempts > retries:
        raise ValueError(
            'Value passed for --preemptible cannot be larger than --retries.')


def preemptile_param_type(preemptible):
  """Wrapper function to create a PreemptibleParam object from argparse."""
  if bool is type(preemptible):
    return PreemptibleParam(preemptible)
  elif str is type(preemptible):
    try:
      return PreemptibleParam(int(preemptible))
    except ValueError:
      raise argparse.ArgumentTypeError(
          'Invalid value {} for --preemptible.'.format(preemptible))
  else:
    raise argparse.ArgumentTypeError(
        'Invalid value {} for --preemptible.'.format(preemptible))
