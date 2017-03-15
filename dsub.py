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

"""Submit batch jobs.

Follows the model of bsub, qsub, srun, etc.
"""

import argparse
import collections
import csv
import os
import re
import sys
import time

from lib import dsub_util
from providers import provider_base

DEFAULT_SCOPES = ['https://www.googleapis.com/auth/bigquery',]

# The job created by dsub will automatically include a data disk,
# Each provider sets a different DATA_ROOT environment variable.
# The DATA_ROOT is the root directory for the data disk.
#
# Input arguments will, by default, be localized to ${DATA_ROOT}/input.
# Output arguments will, by default, be de-localized from ${DATA_ROOT}/output.
#
# The local paths will be automatically set to mirror the remote path.
# For example:
#    gs://bucket/path/file
#
# will become:
#    ${DATA_ROOT}/input/gs/bucket/path/file
#
# On the command-line the file arguments can be specified as either:
#
#   remote_uri
#   NAME=remote_uri
#
# If no NAME is specified, one is automatically generated, of the form
# INPUT_<n> where <n> starts at 0 for the first parsed input parameter.
#
# Example inputs:
#
# * to copy a set of BAM files into ${DATA_ROOT}/input, set:
#
#   --input gs://bucket/path/*.bam
#
# The contents will be written to (and the input parameter set to):
#
#   ${DATA_ROOT}/input/gs/bucket/path/
#
# * to copy a single BAM file to ${DATA_ROOT}/input, set:
#
#   --input gs://bucket/path/NA12878.bam
#
# The contents will be written to (and the input parameter set to):
#
#   ${DATA_ROOT}/input/gs/bucket/path/NA12878.bam
#
# Example outputs:
#
# * to copy out all BAM index files from ${DATA_ROOT}/output/gs/bucket/path/,
#   set:
#
#   --output gs://bucket/path/*.bai
#
# * to copy out a single BAM index file,
#   ${DATA_ROOT}/output/gs/bucket/path/sample.bam.bai, set:
#
#   --output gs://bucket/path/sample.bam.bai
#
# Similar functionality is available in the header row of a TSV table file:
#
#   --input
#   --input VAR
#
#   --output
#   --output VAR

DEFAULT_INPUT_LOCAL_PATH = 'input'
DEFAULT_OUTPUT_LOCAL_PATH = 'output'

AUTO_PREFIX_INPUT = 'INPUT_'  # Prefix for auto-generated input names
AUTO_PREFIX_OUTPUT = 'OUTPUT_'  # Prefix for auto-generated output names


def print_error(msg):
  """Utility routine to emit messages to stderr."""
  print >> sys.stderr, msg


def split_pair(pair_string, separator, nullable_idx):
  """Split a pair, which can have one empty value."""

  pair = pair_string.split(separator, 1)
  if len(pair) == 1:
    if nullable_idx == 0:
      return [None, pair[0]]
    else:
      return [pair[0], None]
  else:
    return pair


class JobResources(
    collections.namedtuple('JobResources', [
        'min_cores', 'min_ram', 'disk_size', 'boot_disk_size', 'preemptible',
        'image', 'logging', 'zones', 'scopes'
    ])):
  """Job resource parameters related to CPUs, memory, and disk.

  Attributes:
    min_cores (int): number of CPU cores
    min_ram (float): amount of memory (in GB)
    disk_size (int): size of the data disk (in GB)
    boot_disk_size (int): size of the boot disk (in GB)
    preemptible (bool): use a preemptible VM for the job
    image (str): Docker image name
    logging (str): path to location for jobs to write logs
    zones (str): location in which to run the job
    scopes (list): OAuth2 scopes for the job
  """
  __slots__ = ()

  def __new__(cls,
              min_cores=1,
              min_ram=1,
              disk_size=10,
              boot_disk_size=10,
              preemptible=False,
              image=None,
              logging=None,
              zones=None,
              scopes=None):
    return super(JobResources, cls).__new__(cls, min_cores, min_ram, disk_size,
                                            boot_disk_size, preemptible, image,
                                            logging, zones, scopes)


def validate_param_name(name, param_type):
  """Validate that the name follows posix conventions for env variables."""

  # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_235
  #
  # 3.235 Name
  # In the shell command language, a word consisting solely of underscores,
  # digits, and alphabetics from the portable character set.
  if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
    raise ValueError('Invalid %s: %s' % (param_type, name))


class ListParamAction(argparse.Action):
  """Append each value as a separate element to the parser destination.

  This class refines the 'append' action argument.
  For the parameters:

    --myarg val1 val2 --myarg val3

  'append' action yields:

    args.myval = ['val1 val2', 'val3']

  ListParamAction yields:

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
    collections.namedtuple('FileParam',
                           ['name', 'docker_path', 'remote_uri', 'recursive'])):
  """File parameter to be automatically localized or de-localized.

  Input files are automatically localized from GCS to the pipeline VM's
  local block disk(s).

  Output files are automatically de-localized to GCS from the pipeline VM's
  local block disk(s).

  Attributes:
    name (str): the parameter and environment variable name.
    docker_path (str): the on-VM location; also set as the environment variable
        value.
    remote_uri (str): the GCS path.
  """
  __slots__ = ()

  def __new__(cls, name, docker_path=None, remote_uri=None, recursive=False):
    return super(FileParam, cls).__new__(cls, name, docker_path, remote_uri,
                                         recursive)


class InputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls, name, docker_path=None, remote_uri=None, recursive=False):
    validate_param_name(name, 'Input parameter')
    return super(InputFileParam, cls).__new__(cls, name, docker_path,
                                              remote_uri, recursive)


class OutputFileParam(FileParam):
  """Simple typed-derivative of a FileParam."""

  def __new__(cls, name, docker_path=None, remote_uri=None, recursive=False):
    validate_param_name(name, 'Input parameter')
    return super(OutputFileParam, cls).__new__(cls, name, docker_path,
                                               remote_uri, recursive)


class FileParamUtil(object):
  """Base class helper for producing FileParams from args or a table file.

  InputFileParams and OutputFileParams can be produced from either arguments
  passed on the command-line or as a combination of the definition in the table
  header plus cell values in table records.

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
      # produces different results dependening on whether *.bam matches a single
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


class Script(object):
  """Script to be run by for the job.

  The Pipeline's API specifically supports bash commands as the docker
  command. To support any type of script (Python, Ruby, etc.), the contents
  are uploaded as a simple environment variable input parameter.
  The docker command then writes the variable contents to a file and
  executes it.
  """

  def __init__(self, name, value):
    self.name = name
    self.value = value


def parse_arguments(prog, argv):
  """Parses command line arguments.

  Args:
    prog: The path of the program (dsub.py) or an alternate program name to
    display in usage.
    argv: The list of program arguments to parse.

  Returns:
    A Namespace of parsed arguments.
  """
  parser = argparse.ArgumentParser(
      prog=prog, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument(
      '--project',
      required=True,
      help='Cloud project ID in which to run the pipeline')
  parser.add_argument(
      '--logging',
      required=True,
      help='Cloud Storage path to send logging output')
  parser.add_argument(
      '--name',
      help='Name for pipeline. Defaults to the script name or'
      'first token of the --command if specified.')
  parser.add_argument(
      '--min-cores', default=1, type=int, help='Minimum CPU cores for each job')
  parser.add_argument(
      '--min-ram', default=3.75, type=float, help='Minimum RAM per job in GB')
  parser.add_argument(
      '--disk-size',
      default=200,
      type=int,
      help='Size (in GB) of data disk to attach for each job')
  parser.add_argument(
      '--boot-disk-size',
      default=10,
      type=int,
      help='Size (in GB) of the boot disk')
  parser.add_argument(
      '--preemptible',
      default=False,
      action='store_true',
      help='Use a preemptible VM for the job')
  parser.add_argument(
      '--zones',
      default=None,
      nargs='+',
      help='List of Google Compute Engine zones.')
  parser.add_argument(
      '--table',
      default=None,
      help='Path to TSV of job parameters. Each column'
      ' specifies an environment variable to set in each'
      ' jobs\'s parent shell, and each row specifies the values'
      ' of those variables for each job.')
  parser.add_argument(
      '--image',
      default='ubuntu:14.04',
      help='Image name from Docker Hub, Google Container Repository, or other'
      ' Docker image service. The pipeline must have READ access to the image.')
  parser.add_argument(
      '--scopes',
      default=DEFAULT_SCOPES,
      nargs='+',
      help='Space-separated scopes for GCE instances.')
  parser.add_argument(
      '--dry-run',
      default=False,
      action='store_true',
      help='Print the pipeline(s) that would be run and then exit.')
  parser.add_argument(
      '--verbose',
      default=False,
      action='store_true',
      help='Verbose output of parameters and operations?')
  parser.add_argument(
      '--wait',
      action='store_true',
      help='Wait for all operations to complete?')
  parser.add_argument(
      '--poll-interval',
      default=5,
      type=int,
      help='Polling interval for checking operation status')
  parser.add_argument(
      '--env',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Environment variables for the script\'s execution environment',
      metavar='KEY=VALUE')
  parser.add_argument(
      '--input',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Input path arguments to localize into the script\'s execution'
      ' environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--input-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Input path arguments to localize recursively into the script\'s'
      ' execution environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Output path arguments to de-localize from the script\'s execution'
      ' environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--output-recursive',
      nargs='*',
      action=ListParamAction,
      default=[],
      help='Output path arguments to de-localize recursively from the script\'s'
      ' execution environment',
      metavar='KEY=REMOTE_PATH')
  parser.add_argument(
      '--command',
      help='Command to run inside the job\'s Docker container',
      metavar='COMMAND')
  parser.add_argument(
      '--script',
      help='Local path to a script to run inside the job\'s Docker container.',
      metavar='SCRIPT')
  parser.add_argument(
      'deprecated_script', nargs='?', help=argparse.SUPPRESS, metavar='SCRIPT')
  parser.add_argument(
      'params',
      nargs='*',
      default=[],
      help=argparse.SUPPRESS,
      metavar='KEY=VALUE')
  return parser.parse_args(argv)


def get_job_resources(args):
  """Extract job-global resources requirements from input args.

  Args:
    args: parsed command-line arguments

  Returns:
    JobResources object containing the requested resources for the job
  """

  return JobResources(
      min_cores=args.min_cores,
      min_ram=args.min_ram,
      disk_size=args.disk_size,
      boot_disk_size=args.boot_disk_size,
      preemptible=args.preemptible,
      image=args.image,
      zones=args.zones,
      logging=args.logging,
      scopes=args.scopes)


def get_job_metadata(args, script, provider):
  """Allow provider to extract job-specific metadata from command-line args.

  Args:
    args: parsed command-line arguments
    script: the script to run
    provider: job service provider

  Returns:
    A dictionary of job-specific metadata (such as job id, name, etc.)
  """

  job_metadata = provider.get_job_metadata(script.name, args.name,
                                           dsub_util.get_default_user(),
                                           args.table is not None)
  job_metadata['script'] = script

  return job_metadata


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
    name, value = split_pair(arg, '=', 1)
    env_data.append(EnvParam(name, value))

  # For input files, we need to:
  #   * split the input into name=remote_uri pairs (name optional)
  #   * validate the remote uri
  #   * generate a docker_path
  input_data = []
  for (recursive, args) in ((False, inputs), (True, inputs_recursive)):
    for arg in args:
      name, remote_uri = split_pair(arg, '=', 0)

      name = input_file_param_util.get_variable_name(name)
      docker_path, remote_uri = input_file_param_util.parse_uri(remote_uri,
                                                                recursive)
      input_data.append(
          InputFileParam(name, docker_path, remote_uri, recursive))

  # For output files, we need to:
  #   * split the input into name=remote_uri pairs (name optional)
  #   * validate the remote uri
  #   * generate the remote uri
  #   * generate a docker_path
  output_data = []
  for (recursive, args) in ((False, outputs), (True, outputs_recursive)):
    for arg in args:
      name, remote_uri = split_pair(arg, '=', 0)

      name = output_file_param_util.get_variable_name(name)
      docker_path, remote_uri = output_file_param_util.parse_uri(remote_uri,
                                                                 recursive)
      output_data.append(
          OutputFileParam(name, docker_path, remote_uri, recursive))

  return [{
      'envs': env_data,
      'inputs': input_data,
      'outputs': output_data,
  }]


def parse_job_table_header(header, input_file_param_util,
                           output_file_param_util):
  """Parse the first row of the job table into env, input, output definitions.

  Elements are formatted similar to their equivalent command-line arguments,
  but with associated values coming from the data rows.

  Environment variables columns are headered as "--env <name>"
  Inputs columns are headered as "--input <name>" with the name optional.
  Outputs columns are headered as "--output <name>" with the name optional.

  For historical reasons, bareword column headers (such as "JOB_ID") are
  equivalent to "--env var_name".

  Args:
    header: The first row of the tab-delimited jobs table file.
    input_file_param_util: Utility for producing InputFileParam objects.
    output_file_param_util: Utility for producing OutputFileParam objects.

  Returns:
    job_params: A list of EnvParams and FileParams for the environment
    variables, input file parameters, and output file parameters.

  Raises:
    ValueError: If a header contains a ":" and the prefix is not supported.
  """
  job_params = []

  # Tokenize the header line and process each field
  header_columns = header.split('\t')
  for col in header_columns:

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
          InputFileParam(
              name, recursive=(col_type.endswith('recursive'))))

    elif col_type == '--output' or col_type == '--output-recursive':
      name = output_file_param_util.get_variable_name(col_value)
      job_params.append(
          OutputFileParam(
              name, recursive=(col_type.endswith('recursive'))))

    else:
      raise ValueError('Unrecognized column header: %s' % col)

  return job_params


def table_to_job_data(path, input_file_param_util, output_file_param_util):
  """Parses a table of parameters from a TSV.

  Args:
    path: Path to a TSV file with the first line specifying the environment
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

  with open(path, 'r') as param_file:
    # Read the first line and extract the fieldnames
    job_params = parse_job_table_header(param_file.readline().rstrip(),
                                        input_file_param_util,
                                        output_file_param_util)

    # Parse the rest of the file as job data
    reader = csv.reader(param_file, delimiter='\t')

    # Build a list of records from the parsed input table
    for row in reader:
      if len(row) != len(job_params):
        print_error('Unexpected number of fields %s vs %s: line %s' %
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
              InputFileParam(param.name, docker_path, remote_uri,
                             param.recursive))

        elif isinstance(param, OutputFileParam):
          docker_path, remote_uri = output_file_param_util.parse_uri(
              row[i], param.recursive)
          outputs.append(
              OutputFileParam(param.name, docker_path, remote_uri,
                              param.recursive))

      job_data.append({'envs': envs, 'inputs': inputs, 'outputs': outputs})

  # Ensure that there are jobs to execute (and not just a header)
  if not job_data:
    raise ValueError('No jobs found in %s' % param_file)

  return job_data


def wait_for_operations(provider, job_metadata, poll_interval):
  """Waits for a set of operations to complete.

  Args:
    provider: job service provider
    job_metadata: global metadata for the submitted job
    poll_interval: integer seconds to wait between iterations

  Returns:
    A list of error messages from failed operations.
  """
  user_id = job_metadata['user-id']
  job_id = job_metadata['job-id']

  while True:
    # While waiting for RUNNING operations to complete, we only ned to fetch
    # a single operation each time
    running_ops = provider.get_jobs(
        ['RUNNING'], user_list=[user_id], job_list=[job_id], max_jobs=1)
    if not running_ops:
      break

    time.sleep(poll_interval)

  bad_ops = provider.get_jobs(
      ['FAILURE', 'CANCELED'], user_list=[user_id], job_list=[job_id])
  return provider.get_job_completion_messages(bad_ops)


def call(argv):
  return main('%s.call' % __name__, argv)


def main(prog, argv):
  # Parse args and validate
  args = parse_arguments(prog, argv)

  if args.command and args.script:
    raise ValueError('Cannot supply both --command and a script name')

  if args.deprecated_script:
    print_error('Using a positional argument for the job script is '
                'deprecated.')
    print_error('Use the --script argument instead.')

    # Set the script from the deprecated positional argument.
    args.script = args.deprecated_script

  if args.params:
    print_error('Using positional arguments for input variables is '
                'deprecated.')
    print_error('Use the --env argument instead.')

    # Merge args.params into args.env
    args.env.extend(args.params)

  if (args.env or args.input or args.input_recursive or args.output or
      args.output_recursive) and args.table:
    raise ValueError('Cannot supply both command-line parameters '
                     '(--env/--input/--input-recursive/--output/'
                     '--output-recursive) and --table')

  if args.command:
    if args.name:
      command_name = args.name
    else:
      command_name = args.command.split(' ', 1)[0]
    script = Script(command_name, args.command)
  elif args.script:
    # Read the script file
    with open(args.script, 'r') as script_file:
      script = Script(os.path.basename(args.script), script_file.read())
  else:
    raise ValueError('One of --command or a script name must be supplied')

  # Set up the Genomics Pipelines service interface
  provider = provider_base.get_provider(args)

  # Extract arguments that are global for the batch of jobs to run
  job_resources = get_job_resources(args)
  job_metadata = get_job_metadata(args, script, provider)

  # Set up job parameters and job data from a table file or the command-line
  input_file_param_util = InputFileParamUtil(DEFAULT_INPUT_LOCAL_PATH)
  output_file_param_util = OutputFileParamUtil(DEFAULT_OUTPUT_LOCAL_PATH)
  if args.table:
    all_job_data = table_to_job_data(args.table, input_file_param_util,
                                     output_file_param_util)
  else:
    all_job_data = args_to_job_data(args.env, args.input, args.input_recursive,
                                    args.output, args.output_recursive,
                                    input_file_param_util,
                                    output_file_param_util)

  if not args.dry_run:
    print 'Job: %s' % job_metadata['job-id']

  # Launch all the job tasks!
  launched_job = provider.submit_job(job_resources, job_metadata, all_job_data)

  if not args.dry_run:
    print 'Launched job-id: %s' % launched_job['job-id']
    print '        user-id: %s' % launched_job['user-id']
    if launched_job.get('task-id'):
      for task_id in launched_job['task-id']:
        print '  Task: %s' % task_id

  # Poll for operation completion
  if args.wait:
    print 'Waiting for jobs to complete...'

    error_messages = wait_for_operations(provider, job_metadata,
                                         args.poll_interval)
    if error_messages:
      for msg in error_messages:
        print_error(msg)
      sys.exit(1)

  # Return a list of the jobs/tasks
  return launched_job


if __name__ == '__main__':
  main(sys.argv[0], sys.argv[1:])
