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
"""Tests for dsub.lib.param_util."""

import datetime
import doctest
import os
import re
import unittest
from dsub.lib import dsub_util
from dsub.lib import job_model
from dsub.lib import param_util
import parameterized
import pytz


# Fixed values for age_to_create_time
FIXED_TIME = datetime.datetime(2017, 1, 1)
FIXED_TIME_UTC = int(
    (FIXED_TIME - datetime.datetime.utcfromtimestamp(0)).total_seconds())


class ParamUtilTest(unittest.TestCase):

  def testParseTasksFileHeader(self):
    header = '--env SAMPLE_ID\t--input VCF_FILE\t--output-recursive OUTPUT_PATH'
    header = header.split('\t')
    input_file_param_util = param_util.InputFileParamUtil('input')
    output_file_param_util = param_util.OutputFileParamUtil('output')
    job_params = param_util.parse_tasks_file_header(
        header, input_file_param_util, output_file_param_util)
    self.assertEqual(3, len(job_params))

    self.assertIsInstance(job_params[0], job_model.EnvParam,
                          'Ensure first parameter is the SAMPLE env param')
    self.assertIsInstance(job_params[0], job_model.EnvParam)
    self.assertEqual('SAMPLE_ID', job_params[0].name)

    self.assertIsInstance(job_params[1], job_model.InputFileParam)
    self.assertEqual('VCF_FILE', job_params[1].name)
    self.assertFalse(job_params[1].recursive)

    self.assertIsInstance(job_params[2], job_model.OutputFileParam)
    self.assertEqual('OUTPUT_PATH', job_params[2].name)
    self.assertTrue(job_params[2].recursive)

  def testTasksFileToTaskDescriptors(self):
    testpath = os.path.dirname(__file__)
    expected_tsv_file = os.path.join(testpath, '../testdata/params_tasks.tsv')
    input_file_param_util = param_util.InputFileParamUtil('input')
    output_file_param_util = param_util.OutputFileParamUtil('output')
    all_task_descriptors = param_util.tasks_file_to_task_descriptors({
        'path': expected_tsv_file
    }, 0, input_file_param_util, output_file_param_util)
    self.assertEqual(4, len(all_task_descriptors))

    for i in range(4):
      task_params = all_task_descriptors[i].task_params
      task_env = task_params['envs'].pop()
      task_input = task_params['inputs'].pop()
      task_output = task_params['outputs'].pop()

      self.assertEqual('SAMPLE_ID', task_env.name)
      self.assertEqual('sid-00%d' % i, task_env.value)
      self.assertEqual('VCF_FILE', task_input.name)
      self.assertEqual('input/gs/inputs/sid-00%d.vcf' % i,
                       task_input.docker_path)
      self.assertEqual('OUTPUT_PATH', task_output.name)
      self.assertEqual('output/gs/outputs/results-00%d/' % i,
                       task_output.docker_path)

  @parameterized.parameterized.expand([
      ('simple_second', '1s', FIXED_TIME - datetime.timedelta(seconds=1)),
      ('simple_minute', '1m', FIXED_TIME - datetime.timedelta(minutes=1)),
      ('simple_hour', '1h', FIXED_TIME - datetime.timedelta(hours=1)),
      ('simple_day', '1d', FIXED_TIME - datetime.timedelta(days=1)),
      ('simple_week', '1w', FIXED_TIME - datetime.timedelta(weeks=1)),
      ('simple_now', str(FIXED_TIME_UTC),
       dsub_util.replace_timezone(FIXED_TIME, pytz.utc)),
  ])
  def test_compute_create_time(self, unused_name, age, expected):
    del unused_name
    result = param_util.age_to_create_time(age, FIXED_TIME)
    self.assertEqual(expected, result)

  @parameterized.parameterized.expand([
      ('bad_units', '1second'),
      ('overflow', '100000000w'),
  ])
  def test_compute_create_time_fail(self, unused_name, age):
    del unused_name
    with unittest.TestCase.assertRaisesRegex(self, ValueError,
                                             'Unable to parse age string'):
      _ = param_util.age_to_create_time(age)

  @parameterized.parameterized.expand([
      ('simple_second', '1s', '1.0s'),
      ('simple_minute', '1m', '60.0s'),
      ('simple_hour', '1h', '3600.0s'),
      ('simple_day', '1d', '86400.0s'),
      ('simple_week', '1w', '604800.0s'),
  ])
  def test_timeout_in_seconds(self, unused_name, timeout, expected):
    del unused_name
    result = param_util.timeout_in_seconds(timeout)
    self.assertEqual(expected, result)

  @parameterized.parameterized.expand([
      ('bad_units', '1second'),
      ('no_units', '123'),
  ])
  def test_timeout_in_seconds_fail(self, unused_name, timeout):
    del unused_name
    with unittest.TestCase.assertRaisesRegex(self, ValueError,
                                             'Unable to parse interval'):
      _ = param_util.timeout_in_seconds(timeout)

  @parameterized.parameterized.expand([
      ('simple_second', '1s', '1.0s'),
      ('simple_minute', '1m', '60.0s'),
      ('simple_hour', '1h', '3600.0s'),
  ])
  def test_log_interval_in_seconds(self, unused_name, log_interval, expected):
    del unused_name
    result = param_util.log_interval_in_seconds(log_interval)
    self.assertEqual(expected, result)

  @parameterized.parameterized.expand([
      ('simple_day', '1d'),
      ('simple_week', '1w'),
      ('bad_units', '1second'),
      ('no_units', '123'),
  ])
  def test_log_interval_in_seconds_fail(self, unused_name, log_interval):
    del unused_name
    with unittest.TestCase.assertRaisesRegex(self, ValueError,
                                             'Unable to parse interval'):
      _ = param_util.log_interval_in_seconds(log_interval)


class FileParamUtilTest(unittest.TestCase):

  @parameterized.parameterized.expand([
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile', job_model.P_LOCAL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', job_model.P_LOCAL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile',
       job_model.P_LOCAL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile',
       job_model.P_LOCAL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', job_model.P_GCS),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', job_model.P_GCS),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile',
       job_model.P_GCS),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/',
       job_model.P_LOCAL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', job_model.P_LOCAL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/',
       job_model.P_LOCAL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/',
       job_model.P_LOCAL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', job_model.P_GCS),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', job_model.P_GCS),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/',
       job_model.P_GCS),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt',
       job_model.P_GCS),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', job_model.P_GCS),
      ('wc', False, '*.bam', 'file/*.bam', job_model.P_LOCAL),
      ('wc', False, '../*', 'file/_dotdot_/*', job_model.P_LOCAL),
      ('nl', False, '', None, None),
      ('nl', True, '', None, None),
  ])
  def test_input_file_docker_rewrite(self, unused_name, recursive, uri, docker,
                                     provider):
    del unused_name
    if docker:
      docker = os.path.join('input', docker)
    file_param_util = param_util.InputFileParamUtil('input')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, job_model.InputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile', job_model.P_LOCAL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', job_model.P_LOCAL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile',
       job_model.P_LOCAL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile',
       job_model.P_LOCAL),
      ('lf', False, '/a../myfile', 'file/a../myfile', job_model.P_LOCAL),
      ('lf', False, '../myfile', 'file/_dotdot_/myfile', job_model.P_LOCAL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', job_model.P_GCS),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', job_model.P_GCS),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile',
       job_model.P_GCS),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/',
       job_model.P_LOCAL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', job_model.P_LOCAL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/',
       job_model.P_LOCAL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/',
       job_model.P_LOCAL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', job_model.P_GCS),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', job_model.P_GCS),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/',
       job_model.P_GCS),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt',
       job_model.P_GCS),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', job_model.P_GCS),
      ('wc', False, '*.bam', 'file/*.bam', job_model.P_LOCAL),
      ('wc', False, '../*', 'file/_dotdot_/*', job_model.P_LOCAL),
      ('nl', False, '', None, None),
      ('nl', True, '', None, None),
  ])
  def test_out_file_docker_rewrite(self, unused_name, recursive, uri, docker,
                                   provider):
    del unused_name
    if docker:
      docker = os.path.join('output', docker)
    file_param_util = param_util.OutputFileParamUtil('output')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, job_model.OutputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', job_model.P_GCS),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile',
       job_model.P_GCS),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile', job_model.P_LOCAL),
      ('lf', False, '../myfile', '../', 'myfile', job_model.P_LOCAL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', job_model.P_GCS),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', job_model.P_GCS),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*',
       job_model.P_GCS),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', job_model.P_LOCAL),
      ('lfwc', False, './*', './', '*', job_model.P_LOCAL),
      ('localroot', False, '/*', '/', '*', job_model.P_LOCAL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', job_model.P_LOCAL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', job_model.P_LOCAL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', job_model.P_LOCAL),
      ('lr', True, '../myfile', '../myfile/', '', job_model.P_LOCAL),
      ('lr', True, './', './', '', job_model.P_LOCAL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', job_model.P_GCS),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', job_model.P_GCS),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '',
       job_model.P_GCS),
  ])
  def test_uri_rewrite_out(self, unused_name, recursive, raw_uri, path, bn,
                           provider):
    del unused_name
    if provider == job_model.P_LOCAL:
      path = os.path.abspath(path).rstrip('/') + '/'
    out_util = param_util.OutputFileParamUtil('')
    out_param = out_util.make_param('TEST', raw_uri, recursive=recursive)
    self.assertEqual(path, out_param.uri.path)
    self.assertEqual(bn, out_param.uri.basename)
    self.assertEqual(path + bn, out_param.uri)
    self.assertEqual(provider, out_param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', job_model.P_GCS),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile',
       job_model.P_GCS),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile', job_model.P_LOCAL),
      ('lf', False, '../myfile', '../', 'myfile', job_model.P_LOCAL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', job_model.P_GCS),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', job_model.P_GCS),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*',
       job_model.P_GCS),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', job_model.P_LOCAL),
      ('lfwc', False, './*', './', '*', job_model.P_LOCAL),
      ('localroot', False, '/*', '/', '*', job_model.P_LOCAL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', job_model.P_LOCAL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', job_model.P_LOCAL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', job_model.P_LOCAL),
      ('lr', True, '../myfile', '../myfile/', '', job_model.P_LOCAL),
      ('lr', True, './', './', '', job_model.P_LOCAL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', job_model.P_GCS),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', job_model.P_GCS),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '',
       job_model.P_GCS),
  ])
  def test_uri_rewrite_in(self, unused_name, recursive, uri_raw, path, bn,
                          provider):
    del unused_name
    if provider == job_model.P_LOCAL:
      path = os.path.abspath(path).rstrip('/') + '/'
    in_util = param_util.InputFileParamUtil('')
    in_param = in_util.make_param('TEST', uri_raw, recursive=recursive)
    self.assertEqual(path, in_param.uri.path)
    self.assertEqual(bn, in_param.uri.basename)
    self.assertEqual(path + bn, in_param.uri)
    self.assertEqual(provider, in_param.file_provider)

  @parameterized.parameterized.expand([
      ('cant_use_wc', True, 'gs://tmp/myfile/*', 'only supported for files'),
      ('dir_wc', False, 'gs://b/yfile/*/*', 'only supported for files'),
      ('question', False, 'gs://b/myfile/?', 'Question mark'),
      ('recursive_wc', False, 'gs://b/myfile/**', 'Recursive'),
      ('no_filename_l', False, '../myfile/', 'not recursive must reference'),
      ('no_filename_g', False, 'gs://myfile/', 'not recursive must reference'),
  ])
  def test_output_val_err(self, unused_name, recursive, uri, regex):
    del unused_name
    file_param_util = param_util.OutputFileParamUtil('output')
    with unittest.TestCase.assertRaisesRegex(self, ValueError, regex):
      file_param_util.parse_uri(uri, recursive)

  @parameterized.parameterized.expand([
      ('s3', 's3://b/myfile/', 'not supported: s3://'),
      ('gluster', 'gluster+tcp://myfile/', r'supported: gluster\+tcp://'),
      ('ftp', 'ftp://myfile/', 'not supported: ftp://'),
  ])
  def test_file_provider_err(self, unused_name, uri, regex):
    del unused_name
    file_param_util = param_util.OutputFileParamUtil('output')
    with unittest.TestCase.assertRaisesRegex(self, ValueError, regex):
      file_param_util.parse_file_provider(uri)

  @parameterized.parameterized.expand([
      ('l', '/tmp/mydir/inner', '/tmp/mydir/inner/', job_model.P_LOCAL),
      ('l_log', '/tmp/mydir/data.log', '/tmp/mydir/data.log',
       job_model.P_LOCAL),
      ('l_indir', '/tmp/mydir/extra/../runner', '/tmp/mydir/runner/',
       job_model.P_LOCAL),
      ('g', 'gs://bucket/mydir', 'gs://bucket/mydir/', job_model.P_GCS),
      ('glog', 'gs://bucket/my.log', 'gs://bucket/my.log', job_model.P_GCS),
  ])
  def test_logging_param_maker(self, unused_name, uri, expected_out, provider):
    del unused_name
    param = param_util.build_logging_param(
        uri, util_class=param_util.OutputFileParamUtil)
    self.assertEqual(param.uri, expected_out)
    self.assertEqual(param.file_provider, provider)


TASK_DESCRIPTORS = [
    job_model.TaskDescriptor(
        None, {
            'inputs': [
                job_model.FileParam(
                    'IN', uri='gs://in/*', file_provider=job_model.P_GCS)
            ],
            'outputs': []
        }, None),
    job_model.TaskDescriptor(
        None, {
            'inputs': [],
            'outputs': [
                job_model.FileParam(
                    'OUT', uri='gs://out/*', file_provider=job_model.P_GCS)
            ]
        }, None),
]


class TestSubmitValidator(unittest.TestCase):

  def test_submit_validator_passes(self):
    job_params = {'inputs': set(), 'outputs': set(), 'mounts': set()}
    job_resources = job_model.Resources(
        logging=job_model.LoggingParam('gs://buck/logs', job_model.P_GCS))
    param_util.validate_submit_args_or_fail(
        job_model.JobDescriptor(None, job_params, job_resources,
                                TASK_DESCRIPTORS),
        provider_name='MYPROVIDER',
        input_providers=[job_model.P_GCS],
        output_providers=[job_model.P_GCS],
        logging_providers=[job_model.P_GCS])

  @parameterized.parameterized.expand([
      ('input', 'gs://in/*', [job_model.P_LOCAL], [job_model.P_GCS],
       [job_model.P_GCS]),
      ('output', 'gs://out/*', [job_model.P_GCS], [job_model.P_LOCAL],
       [job_model.P_GCS]),
      ('logging', 'gs://buck/logs', [job_model.P_GCS], [job_model.P_GCS],
       [job_model.P_LOCAL]),
  ])
  def test_submit_validator_fails(self, name, path, inwl, outwl, logwl):
    job_params = {'inputs': set(), 'outputs': set(), 'mounts': set()}
    job_resources = job_model.Resources(
        logging=job_model.LoggingParam('gs://buck/logs', job_model.P_GCS))
    err_expected = 'Unsupported %s path (%s) for provider' % (name, path)
    with unittest.TestCase.assertRaisesRegex(self, ValueError,
                                             re.escape(err_expected)):
      param_util.validate_submit_args_or_fail(
          job_model.JobDescriptor(None, job_params, job_resources,
                                  TASK_DESCRIPTORS),
          provider_name='MYPROVIDER',
          input_providers=inwl,
          output_providers=outwl,
          logging_providers=logwl)


class TestParamUtilDocs(unittest.TestCase):

  def test_doctest(self):
    result = doctest.testmod(param_util, report=True)
    self.assertEqual(0, result.failed)


if __name__ == '__main__':
  unittest.main()
