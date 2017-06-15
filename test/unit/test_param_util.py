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
import unittest
from dsub.lib import param_util


class ParamUtilTest(unittest.TestCase):

  def testEnvParam(self):
    env_param = param_util.EnvParam('my_name', 'my_value')
    self.assertEqual('my_name', env_param.name)
    self.assertEqual('my_value', env_param.value)

  def testFileParam(self):
    file_param = param_util.FileParam(
        'my_name',
        'my_value',
        'my_docker_path',
        'my_remote_uri',
        recursive=True)
    self.assertEqual('my_name', file_param.name)
    self.assertEqual('my_docker_path', file_param.docker_path)
    self.assertEqual('my_remote_uri', file_param.remote_uri)
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


if __name__ == '__main__':
  unittest.main()
