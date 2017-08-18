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

PL = param_util.P_LOCAL
PG = param_util.P_GCS


class ParamUtilTest(unittest.TestCase):

  def testEnvParam(self):
    env_param = param_util.EnvParam('my_name', 'my_value')
    self.assertEqual('my_name', env_param.name)
    self.assertEqual('my_value', env_param.value)

  def testLabelParam(self):
    good_labels = [('genre', 'jazz'), ('underscores_are',
                                       'totally_ok'), ('dashes-are', 'also-ok'),
                   ('num_123', 'good_456'), ('final_underscore_', 'ok_too_'),
                   ('final-dash', 'no-problem-'), ('optional_value',
                                                   ''), ('a' * 63,
                                                         'not_too_long')]
    for name, value in good_labels:
      label_param = param_util.LabelParam(name, value)
      self.assertEqual(name, label_param.name)
      self.assertEqual(value, label_param.value)
    bad_labels = [('WHATS',
                   'updog'), ('1', 'initial_number'), ('initial_number', '1'),
                  ('-', 'initial_dash'), ('initial_dash', '-'),
                  ('spaces bad', ''), ('midCaps', 'bad'), ('bad', 'midCaps'),
                  ('a' * 64, 'too_long'), ('', 'name_required'), ('too_long',
                                                                  'a' * 64)]
    for name, value in bad_labels:
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

    # The first one is the SAMPLE env param.
    self.assertTrue(isinstance(job_params[0], param_util.EnvParam))
    self.assertEqual('SAMPLE_ID', job_params[0].name)

    self.assertTrue(isinstance(job_params[1], param_util.InputFileParam))
    self.assertEqual('VCF_FILE', job_params[1].name)
    self.assertFalse(job_params[1].recursive)

    self.assertTrue(isinstance(job_params[2], param_util.OutputFileParam))
    self.assertEqual('OUTPUT_PATH', job_params[2].name)
    self.assertTrue(job_params[2].recursive)

  def testTasksFileToJobData(self):
    expected_tsv_file = 'test/testdata/params_tasks.tsv'
    input_file_param_util = param_util.InputFileParamUtil('input')
    output_file_param_util = param_util.OutputFileParamUtil('output')
    all_job_data = param_util.tasks_file_to_job_data({
        'path': expected_tsv_file
    }, input_file_param_util, output_file_param_util)
    self.assertEqual(4, len(all_job_data))

    for i in range(4):
      job_data = all_job_data[i]
      self.assertEqual('SAMPLE_ID', job_data['envs'][0].name)
      self.assertEqual('sid-00%d' % i, job_data['envs'][0].value)
      self.assertEqual('VCF_FILE', job_data['inputs'][0].name)
      self.assertEqual('input/gs/inputs/sid-00%d.vcf' % i,
                       job_data['inputs'][0].docker_path)
      self.assertEqual('OUTPUT_PATH', job_data['outputs'][0].name)
      self.assertEqual('output/gs/outputs/results-00%d/' % i,
                       job_data['outputs'][0].docker_path)

  # Fixed values for age_to_create_time
  fixed_time = datetime.datetime(2017, 1, 1)
  fixed_time_utc = int(
      (fixed_time - datetime.datetime.utcfromtimestamp(0)).total_seconds())

  @parameterized.parameterized.expand([
      ('simple_second', '1s', fixed_time_utc - 1),
      ('simple_minute', '1m', fixed_time_utc - (1 * 60)),
      ('simple_hour', '1h', fixed_time_utc - (1 * 60 * 60)),
      ('simple_day', '1d', fixed_time_utc - (24 * 60 * 60)),
      ('simple_week', '1w', fixed_time_utc - (7 * 24 * 60 * 60)),
      ('simple_now', str(fixed_time_utc), fixed_time_utc),
  ])
  def test_compute_create_time(self, unused_name, age, expected):
    result = param_util.age_to_create_time(age, self.fixed_time)
    self.assertEqual(expected, result)

  @parameterized.parameterized.expand([
      ('bad_units', '1second'),
      ('overflow', '100000000w'),
  ])
  def test_compute_create_time_fail(self, unused_name, age):
    with self.assertRaisesRegexp(ValueError, 'Unable to parse age string'):
      _ = param_util.age_to_create_time(age)


class FileParamUtilTest(unittest.TestCase):

  @parameterized.parameterized.expand([
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile', PL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', PL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile', PL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile', PL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', PG),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', PG),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile', PG),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/', PL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', PL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/', PL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/', PL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', PG),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', PG),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/', PG),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt', PG),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', PG),
      ('wc', False, '*.bam', 'file/*.bam', PL),
      ('wc', False, '../*', 'file/_dotdot_/*', PL),
  ])
  def test_input_file_docker_rewrite(self, _, recursive, uri, docker, provider):
    docker = os.path.join('input', docker)
    file_param_util = param_util.InputFileParamUtil('input')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, param_util.InputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('lf', False, 'file:///tmp/myfile', 'file/tmp/myfile', PL),
      ('lf', False, '/tmp/myfile', 'file/tmp/myfile', PL),
      ('lf', False, '../../myfile', 'file/_dotdot_/_dotdot_/myfile', PL),
      ('lf', False, '~/tmp/myfile', 'file/_home_/tmp/myfile', PL),
      ('lf', False, '/a../myfile', 'file/a../myfile', PL),
      ('lf', False, '../myfile', 'file/_dotdot_/myfile', PL),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', PG),
      ('gf', False, 'gs://tmp/myfile', 'gs/tmp/myfile', PG),
      ('gf', False, 'gs://bucket/../myfile', 'gs/bucket/../myfile', PG),
      # Recursive tests for local and google
      ('lr', True, 'file:///tmp/myfile/', 'file/tmp/myfile/', PL),
      ('lr', True, '/tmp/myfile', 'file/tmp/myfile/', PL),
      ('lr', True, '../../myfile/', 'file/_dotdot_/_dotdot_/myfile/', PL),
      ('lr', True, '~/tmp/myfile', 'file/_home_/tmp/myfile/', PL),
      ('gr', True, 'gs://tmp/myfile/', 'gs/tmp/myfile/', PG),
      ('gr', True, 'gs://tmp/myfile', 'gs/tmp/myfile/', PG),
      ('gr', True, 'gs://bucket/../myfile', 'gs/bucket/../myfile/', PG),
      # wildcard tests for local and google.
      ('wc', False, 'gs://bucket/f/*.txt', 'gs/bucket/f/*.txt', PG),
      ('wc', False, 'gs://bucket/f/*', 'gs/bucket/f/*', PG),
      ('wc', False, '*.bam', 'file/*.bam', PL),
      ('wc', False, '../*', 'file/_dotdot_/*', PL),
  ])
  def test_out_file_docker_rewrite(self, _, recursive, uri, docker, provider):
    docker = os.path.join('output', docker)
    file_param_util = param_util.OutputFileParamUtil('output')
    param = file_param_util.make_param('TEST', uri, recursive)
    self.assertIsInstance(param, param_util.OutputFileParam)
    self.assertEqual('TEST', param.name)
    self.assertEqual(docker, param.docker_path)
    self.assertEqual(provider, param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', PG),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile', PG),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile', PL),
      ('lf', False, '../myfile', '../', 'myfile', PL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', PG),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', PG),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*', PG),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', PL),
      ('lfwc', False, './*', './', '*', PL),
      ('localroot', False, '/*', '/', '*', PL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', PL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', PL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', PL),
      ('lr', True, '../myfile', '../myfile/', '', PL),
      ('lr', True, './', './', '', PL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', PG),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', PG),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '', PG),
  ])
  def test_uri_rewrite_out(self, _, recursive, raw_uri, path, bn, provider):
    # perpare the path if local.
    if provider == PL:
      path = os.path.abspath(path).rstrip('/') + '/'
    out_util = param_util.OutputFileParamUtil('')
    out_param = out_util.make_param('TEST', raw_uri, recursive=recursive)
    self.assertEqual(path, out_param.uri.path)
    self.assertEqual(bn, out_param.uri.basename)
    self.assertEqual(path + bn, out_param.uri)
    self.assertEqual(provider, out_param.file_provider)

  @parameterized.parameterized.expand([
      # Non-recursive tests for local and google
      ('gf', False, 'gs://tmp/myfile', 'gs://tmp/', 'myfile', PG),
      ('gf', False, 'gs://buc/../myfile', 'gs://buc/../', 'myfile', PG),
      ('lf', False, 'file:///tmp/myfile', '/tmp/', 'myfile', PL),
      ('lf', False, '../myfile', '../', 'myfile', PL),
      # Tests with wildcards.
      ('gfwc', False, 'gs://tmp/*.bam', 'gs://tmp/', '*.bam', PG),
      ('gfwc', False, 'gs://tmp/*', 'gs://tmp/', '*', PG),
      ('gfwc', False, 'gs://bucket/../*', 'gs://bucket/../', '*', PG),
      ('lfwc', False, '../tmp/*.bam', '../tmp/', '*.bam', PL),
      ('lfwc', False, './*', './', '*', PL),
      ('localroot', False, '/*', '/', '*', PL),
      ('lfwc', False, '/tmp/*', '/tmp/', '*', PL),
      ('lfwc', False, '/bucket/*', '/bucket/', '*', PL),
      # Recursive tests for local and google
      ('lr', True, '/tmp/myfile/', '/tmp/myfile/', '', PL),
      ('lr', True, '../myfile', '../myfile/', '', PL),
      ('lr', True, './', './', '', PL),
      ('gr', True, 'gs://t/myfile/', 'gs://t/myfile/', '', PG),
      ('gr', True, 'gs://t/myfile', 'gs://t/myfile/', '', PG),
      ('gr', True, 'gs://buc/../myfile', 'gs://buc/../myfile/', '', PG),
  ])
  def test_uri_rewrite_in(self, _, recursive, uri_raw, path, bn, provider):
    # perpare the path if local.
    if provider == PL:
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
    file_param_util = param_util.OutputFileParamUtil('output')
    with self.assertRaisesRegexp(ValueError, regex):
      file_param_util.parse_uri(uri, recursive)

  @parameterized.parameterized.expand([
      ('s3', 's3://b/myfile/', 'not supported: s3://'),
      ('gluster', 'gluster+tcp://myfile/', r'supported: gluster\+tcp://'),
      ('ftp', 'ftp://myfile/', 'not supported: ftp://'),
  ])
  def test_file_provider_err(self, unused_name, uri, regex):
    file_param_util = param_util.OutputFileParamUtil('output')
    with self.assertRaisesRegexp(ValueError, regex):
      file_param_util.parse_file_provider(uri)

  @parameterized.parameterized.expand([
      ('l', '/tmp/mydir/inner', '/tmp/mydir/inner/', PL),
      ('l_log', '/tmp/mydir/data.log', '/tmp/mydir/data.log', PL),
      ('l_indir', '/tmp/mydir/extra/../runner', '/tmp/mydir/runner/', PL),
      ('g', 'gs://bucket/mydir', 'gs://bucket/mydir/', PG),
      ('glog', 'gs://bucket/my.log', 'gs://bucket/my.log', PG),
  ])
  def test_logging_param_maker(self, unused_name, uri, expected_out, provider):
    param = param_util.build_logging_param(
        uri, util_class=param_util.OutputFileParamUtil)
    self.assertEqual(param.uri, expected_out)
    self.assertEqual(param.file_provider, provider)


class TestSubmitValidator(unittest.TestCase):

  def setUp(self):
    self.task_data = [
        {
            'inputs': [
                param_util.FileParam('IN', uri='gs://in/*', file_provider=PG)
            ]
        },
        {
            'outputs': [
                param_util.FileParam('OUT', uri='gs://out/*', file_provider=PG)
            ]
        },
    ]

  def test_submit_validator_passes(self):
    resources = job_util.JobResources(logging=param_util.LoggingParam(
        'gs://buck/logs', PG))
    param_util.validate_submit_args_or_fail(
        job_resources=resources,
        all_task_data=self.task_data,
        provider_name='MYPROVIDER',
        input_providers=[PG],
        output_providers=[PG],
        logging_providers=[PG])

  @parameterized.parameterized.expand([
      ('input', 'gs://in/*', [PL], [PG], [PG]),
      ('output', 'gs://out/*', [PG], [PL], [PG]),
      ('logging', 'gs://buck/logs', [PG], [PG], [PL]),
  ])
  def test_submit_validator_fails(self, name, path, inwl, outwl, logwl):
    resources = job_util.JobResources(logging=param_util.LoggingParam(
        'gs://buck/logs', PG))
    err_expected = 'Unsupported %s path (%s) for provider' % (name, path)
    with self.assertRaisesRegexp(ValueError, re.escape(err_expected)):
      param_util.validate_submit_args_or_fail(
          job_resources=resources,
          all_task_data=self.task_data,
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
