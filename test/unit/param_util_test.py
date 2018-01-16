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

from __future__ import absolute_import
from __future__ import print_function

import datetime
import doctest
import os
import re
import unittest
from dsub.lib import job_util
from dsub.lib import param_util
import parameterized
import pytz


# Fixed values for age_to_create_time
FIXED_TIME = datetime.datetime(2017, 1, 1)
FIXED_TIME_UTC = int(
    (FIXED_TIME - datetime.datetime.utcfromtimestamp(0)).total_seconds())


class ParamUtilTest(unittest.TestCase):

  def testEnvParam(self):
    env_param = param_util.EnvParam('my_name', 'my_value')
    self.assertEqual('my_name', env_param.name)
    self.assertEqual('my_value', env_param.value)

  @parameterized.parameterized.expand([
      ('gl1', 'genre', 'jazz'),
      ('gl2', 'underscores_are', 'totally_ok'),
      ('gl3', 'dashes-are', 'also-ok'),
      ('gl4', 'num_123', 'good_456'),
      ('gl5', 'final_underscore_', 'ok_too_'),
      ('gl6', 'final-dash', 'no-problem-'),
      ('gl7', 'optional_value', ''),
      ('gl8', 'optional_value_2', None),
      ('gl9', 'a' * 63, 'not_too_long'),
      ('gl10', 'numbers-are-now-okay', '1'),
      ('gl11', 'zero-is-okay', '0'),
      ('gl12', 'initial_dash', '-abc'),
      ('gl13', 'initial_underscore', '_abc'),
  ])
  def test_good_labels(self, unused_name, name, value):
    del unused_name
    label_param = param_util.LabelParam(name, value)
    self.assertEqual(name, label_param.name)
    self.assertEqual(value, label_param.value)

  @parameterized.parameterized.expand(
      [('bl1', 'WHATS', 'updog'),
       ('bl2', '1', 'initial_number'),
       ('bl4', '-', 'initial_dash'),
       ('bl6', 'spaces bad', ''),
       ('bl7', 'midCaps', 'bad'),
       ('bl8', 'bad', 'midCaps'),
       ('bl9', 'a' * 64, 'too_long'),
       ('bl10', '', 'name_required'),
       ('bl11', 'too_long', 'a' * 64)])  # pyformat: disable
  def test_bad_labels(self, unused_name, name, value):
    del unused_name
    with self.assertRaises(ValueError):
      param_util.LabelParam(name, value)

  def testFileParam(self):
    file_param = param_util.FileParam(
        'my_name',
        'my_value',
        'my_docker_path',
        'my_remote_uri',
        recursive=True,
        file_provider=param_util.P_GCS)
    self.assertEqual('my_name', file_param.name)
    self.assertEqual('my_docker_path', file_param.docker_path)
    self.assertEqual('my_remote_uri', file_param.uri)
    self.assertTrue(file_param.recursive)

  def testParseTasksFileHeader(self):
    header = '--env SAMPLE_ID\t--input VCF_FILE\t--output-recursive OUTPUT_PATH'
    header = header.split('\t')
    input_file_param_util = param_util.InputFileParamUtil('input')
    output_file_param_util = param_util.OutputFileParamUtil('output')
    job_params = param_util.parse_tasks_file_header(
        header, input_file_param_util, output_file_param_util)
    self.assertEqual(3, len(job_params))

    self.assertIsInstance(job_params[0], param_util.EnvParam,
                          'Ensure first parameter is the SAMPLE env param')
    self.assertTrue(isinstance(job_params[0], param_util.EnvParam))
    self.assertEqual('SAMPLE_ID', job_params[0].name)

    self.assertTrue(isinstance(job_params[1], param_util.InputFileParam))
    self.assertEqual('VCF_FILE', job_params[1].name)
    self.assertFalse(job_params[1].recursive)

    self.assertTrue(isinstance(job_params[2], param_util.OutputFileParam))
    self.assertEqual('OUTPUT_PATH', job_params[2].name)
    self.assertTrue(job_params[2].recursive)

  def testTasksFileToJobData(self):
    testpath = os.path.dirname(__file__)
    expected_tsv_file = os.path.join(testpath, '../testdata/params_tasks.tsv')
    input_file_param_util = param_util.InputFileParamUtil('input')
    output_file_param_util = param_util.OutputFileParamUtil('output')
    all_job_data = param_util.tasks_file_to_job_data({
        'path': expected_tsv_file
    }, input_file_param_util, output_file_param_util)
    self.assertEqual(4, len(all_job_data))

    for i in range(4):
      job_data = all_job_data[i]
      env = job_data['envs'].pop()
      input = job_data['inputs'].pop()
      output = job_data['outputs'].pop()

      self.assertEqual('SAMPLE_ID', env.name)
      self.assertEqual('sid-00%d' % i, env.value)
      self.assertEqual('VCF_FILE', input.name)
      self.assertEqual('input/gs/inputs/sid-00%d.vcf' % i, input.docker_path)
      self.assertEqual('OUTPUT_PATH', output.name)
      self.assertEqual('output/gs/outputs/results-00%d/' % i,
                       output.docker_path)

  @parameterized.parameterized.expand([
      ('simple_second', '1s', FIXED_TIME - datetime.timedelta(seconds=1)),
      ('simple_minute', '1m', FIXED_TIME - datetime.timedelta(minutes=1)),
      ('simple_hour', '1h', FIXED_TIME - datetime.timedelta(hours=1)),
      ('simple_day', '1d', FIXED_TIME - datetime.timedelta(days=1)),
      ('simple_week', '1w', FIXED_TIME - datetime.timedelta(weeks=1)),
      ('simple_now', str(FIXED_TIME_UTC), FIXED_TIME.replace(tzinfo=pytz.utc)),
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
    with self.assertRaisesRegexp(ValueError, 'Unable to parse age string'):
      _ = param_util.age_to_create_time(age)


class FileParamUtilTest(unittest.TestCase):

  @parameterized.parameterized.expand([
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile',
       param_util.P_LOCAL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', param_util.P_LOCAL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile',
       param_util.P_LOCAL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile',
       param_util.P_LOCAL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', param_util.P_GCS),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', param_util.P_GCS),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile',
       param_util.P_GCS),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/',
       param_util.P_LOCAL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', param_util.P_LOCAL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/',
       param_util.P_LOCAL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/',
       param_util.P_LOCAL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', param_util.P_GCS),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', param_util.P_GCS),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/',
       param_util.P_GCS),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt',
       param_util.P_GCS),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', param_util.P_GCS),
      ('wc', False, '*.bam', 'file/*.bam', param_util.P_LOCAL),
      ('wc', False, '../*', 'file/_dotdot_/*', param_util.P_LOCAL),
  ])
  def test_input_file_docker_rewrite(self, unused_name, recursive, uri, docker, provider):
    del unused_name
    docker = os.path.join('input', docker)
    file_param_util = param_util.InputFileParamUtil('input')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, param_util.InputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile',
       param_util.P_LOCAL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', param_util.P_LOCAL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile',
       param_util.P_LOCAL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile',
       param_util.P_LOCAL),
      ('lf', False, '/a../myfile', 'file/a../myfile', param_util.P_LOCAL),
      ('lf', False, '../myfile', 'file/_dotdot_/myfile', param_util.P_LOCAL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', param_util.P_GCS),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', param_util.P_GCS),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile',
       param_util.P_GCS),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/',
       param_util.P_LOCAL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', param_util.P_LOCAL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/',
       param_util.P_LOCAL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/',
       param_util.P_LOCAL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', param_util.P_GCS),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', param_util.P_GCS),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/',
       param_util.P_GCS),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt',
       param_util.P_GCS),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', param_util.P_GCS),
      ('wc', False, '*.bam', 'file/*.bam', param_util.P_LOCAL),
      ('wc', False, '../*', 'file/_dotdot_/*', param_util.P_LOCAL),
  ])
  def test_out_file_docker_rewrite(self, unused_name, recursive, uri, docker, provider):
    del unused_name
    docker = os.path.join('output', docker)
    file_param_util = param_util.OutputFileParamUtil('output')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, param_util.OutputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', param_util.P_GCS),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile',
       param_util.P_GCS),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile',
       param_util.P_LOCAL),
      ('lf', False, '../myfile', '../', 'myfile', param_util.P_LOCAL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', param_util.P_GCS),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', param_util.P_GCS),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*',
       param_util.P_GCS),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', param_util.P_LOCAL),
      ('lfwc', False, './*', './', '*', param_util.P_LOCAL),
      ('localroot', False, '/*', '/', '*', param_util.P_LOCAL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', param_util.P_LOCAL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', param_util.P_LOCAL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', param_util.P_LOCAL),
      ('lr', True, '../myfile', '../myfile/', '', param_util.P_LOCAL),
      ('lr', True, './', './', '', param_util.P_LOCAL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', param_util.P_GCS),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', param_util.P_GCS),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '',
       param_util.P_GCS),
  ])
  def test_uri_rewrite_out(self, unused_name, recursive, raw_uri, path, bn, provider):
    del unused_name
    if provider == param_util.P_LOCAL:
      path = os.path.abspath(path).rstrip('/') + '/'
    out_util = param_util.OutputFileParamUtil('')
    out_param = out_util.make_param('TEST', raw_uri, recursive=recursive)
    self.assertEqual(path, out_param.uri.path)
    self.assertEqual(bn, out_param.uri.basename)
    self.assertEqual(path + bn, out_param.uri)
    self.assertEqual(provider, out_param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', param_util.P_GCS),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile',
       param_util.P_GCS),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile',
       param_util.P_LOCAL),
      ('lf', False, '../myfile', '../', 'myfile', param_util.P_LOCAL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', param_util.P_GCS),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', param_util.P_GCS),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*',
       param_util.P_GCS),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', param_util.P_LOCAL),
      ('lfwc', False, './*', './', '*', param_util.P_LOCAL),
      ('localroot', False, '/*', '/', '*', param_util.P_LOCAL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', param_util.P_LOCAL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', param_util.P_LOCAL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', param_util.P_LOCAL),
      ('lr', True, '../myfile', '../myfile/', '', param_util.P_LOCAL),
      ('lr', True, './', './', '', param_util.P_LOCAL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', param_util.P_GCS),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', param_util.P_GCS),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '',
       param_util.P_GCS),
  ])
  def test_uri_rewrite_in(self, unused_name, recursive, uri_raw, path, bn, provider):
    del unused_name
    if provider == param_util.P_LOCAL:
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
    with self.assertRaisesRegexp(ValueError, regex):
      file_param_util.parse_uri(uri, recursive)

  @parameterized.parameterized.expand([
      ('s3', 's3://b/myfile/', 'not supported: s3://'),
      ('gluster', 'gluster+tcp://myfile/', r'supported: gluster\+tcp://'),
      ('ftp', 'ftp://myfile/', 'not supported: ftp://'),
  ])
  def test_file_provider_err(self, unused_name, uri, regex):
    del unused_name
    file_param_util = param_util.OutputFileParamUtil('output')
    with self.assertRaisesRegexp(ValueError, regex):
      file_param_util.parse_file_provider(uri)

  @parameterized.parameterized.expand([
      ('l', '/tmp/mydir/inner', '/tmp/mydir/inner/', param_util.P_LOCAL),
      ('l_log', '/tmp/mydir/data.log', '/tmp/mydir/data.log',
       param_util.P_LOCAL),
      ('l_indir', '/tmp/mydir/extra/../runner', '/tmp/mydir/runner/',
       param_util.P_LOCAL),
      ('g', 'gs://bucket/mydir', 'gs://bucket/mydir/', param_util.P_GCS),
      ('glog', 'gs://bucket/my.log', 'gs://bucket/my.log', param_util.P_GCS),
  ])
  def test_logging_param_maker(self, unused_name, uri, expected_out, provider):
    del unused_name
    param = param_util.build_logging_param(
        uri, util_class=param_util.OutputFileParamUtil)
    self.assertEqual(param.uri, expected_out)
    self.assertEqual(param.file_provider, provider)



TASK_DATA = [
    {
        'inputs': [
            param_util.FileParam(
                'IN', uri='gs://in/*', file_provider=param_util.P_GCS)
        ],
        'outputs': []
    },
    {
        'inputs': [],
        'outputs': [
            param_util.FileParam(
                'OUT', uri='gs://out/*', file_provider=param_util.P_GCS)
        ]
    },
]


class TestSubmitValidator(unittest.TestCase):

  def test_submit_validator_passes(self):
    resources = job_util.JobResources(
        logging=param_util.LoggingParam('gs://buck/logs', param_util.P_GCS))
    param_util.validate_submit_args_or_fail(
        job_resources=resources,
        job_data={'inputs': [],
                  'outputs': []},
        all_task_data=TASK_DATA,
        provider_name='MYPROVIDER',
        input_providers=[param_util.P_GCS],
        output_providers=[param_util.P_GCS],
        logging_providers=[param_util.P_GCS])

  @parameterized.parameterized.expand([
      ('input', 'gs://in/*', [param_util.P_LOCAL], [param_util.P_GCS],
       [param_util.P_GCS]),
      ('output', 'gs://out/*', [param_util.P_GCS], [param_util.P_LOCAL],
       [param_util.P_GCS]),
      ('logging', 'gs://buck/logs', [param_util.P_GCS], [param_util.P_GCS],
       [param_util.P_LOCAL]),
  ])
  def test_submit_validator_fails(self, name, path, inwl, outwl, logwl):
    resources = job_util.JobResources(
        logging=param_util.LoggingParam('gs://buck/logs', param_util.P_GCS))
    err_expected = 'Unsupported %s path (%s) for provider' % (name, path)
    with self.assertRaisesRegexp(ValueError, re.escape(err_expected)):
      param_util.validate_submit_args_or_fail(
          job_resources=resources,
          job_data={'inputs': [],
                    'outputs': []},
          all_task_data=TASK_DATA,
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
