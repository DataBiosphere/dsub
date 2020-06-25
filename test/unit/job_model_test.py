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
"""Tests for dsub.lib.job_model."""

from __future__ import absolute_import
from __future__ import print_function

import datetime
import doctest
import textwrap
import unittest
from dsub.lib import dsub_util
from dsub.lib import job_model
import parameterized
import pytz

CREATE_TIME_STR = '2017-11-22 14:28:37.321788-08:00'
CREATE_TIME = dsub_util.replace_timezone(
    datetime.datetime.strptime('2017-11-22 22:28:37.321788',
                               '%Y-%m-%d %H:%M:%S.%f'), pytz.utc)


class JobModelTest(unittest.TestCase):

  def testEnvParam(self):
    env_param = job_model.EnvParam('my_name', 'my_value')
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
    label_param = job_model.LabelParam(name, value)
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
      job_model.LabelParam(name, value)

  def testFileParam(self):
    file_param = job_model.FileParam(
        'my_name',
        'my_value',
        'my_docker_path',
        'my_remote_uri',
        recursive=True,
        file_provider=job_model.P_GCS)
    self.assertEqual('my_name', file_param.name)
    self.assertEqual('my_docker_path', file_param.docker_path)
    self.assertEqual('my_remote_uri', file_param.uri)
    self.assertTrue(file_param.recursive)

  def testScriptCreation(self):
    script = job_model.Script('the-name', 'the-value')
    self.assertEqual('the-name', script.name)
    self.assertEqual('the-value', script.value)


#
# Test data for the JobDescriptor tests.
#

# The following test data is from the e2e_env_list test.
#
# All parameters are from the command-line (no --tasks file).
# Thus all meaningful fields should be in the job, rather than the task.
# The only task field should be the task-id, which should be None.
#
# dsub \
#   --logging gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log
#   --image "ubuntu" \
#   --script "${SCRIPT_DIR}/script_env_test.sh" \
#   --env VAR1="VAL1" VAR2="VAL2" VAR3="VAL3" \
#   --env VAR4="VAL4" \
#   --env VAR5="VAL5"

_ENV_LIST_META = textwrap.dedent("""
  create-time: {}
""".format(CREATE_TIME_STR) + """
  dsub-version: 0.1.4
  envs:
    VAR1: VAL1
    VAR2: VAL2
    VAR3: VAL3
    VAR4: VAL4
    VAR5: VAL5
  job-id: script_env--dsubuser--171122-142837-321721
  job-name: script_env_test.sh
  logging: gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log
  tasks:
  - logging-path: gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log
    task-id: null
  user-id: dsubuser
""")

_ENV_LIST_JOB_DESCRIPTOR = job_model.JobDescriptor(
    job_metadata={
        'dsub-version': '0.1.4',
        'job-id': 'script_env--dsubuser--171122-142837-321721',
        'job-name': 'script_env_test.sh',
        'user-id': 'dsubuser',
        'create-time': CREATE_TIME
    },
    job_resources=job_model.Resources(
        logging='gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log'),
    job_params={
        'envs': {
            job_model.EnvParam('VAR1', 'VAL1'),
            job_model.EnvParam('VAR2', 'VAL2'),
            job_model.EnvParam('VAR3', 'VAL3'),
            job_model.EnvParam('VAR4', 'VAL4'),
            job_model.EnvParam('VAR5', 'VAL5'),
        },
        'labels': set(),
        'inputs': set(),
        'outputs': set(),
        'input-recursives': set(),
        'output-recursives': set(),
        'mounts': set(),
    },
    task_descriptors=[
        job_model.TaskDescriptor(
            task_metadata={'task-id': None},
            task_resources=job_model.Resources(
                logging_path='gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log'  # pylint: disable=line-too-long
            ),
            task_params={
                'envs': set(),
                'labels': set(),
                'inputs': set(),
                'outputs': set(),
                'input-recursives': set(),
                'output-recursives': set(),
            })
    ])

# The following test data is from the e2e_io_tasks.sh test.

# This job uses a --tasks file.
#
# dsub \
#   --logging gs://b/dsub/sh/local/env_list/env_list/logging/env_list.log
#   --image "ubuntu" \
#   --script "script_io_test.sh" \
#   --tasks  io_tasks.tsv
#
# where io_tasks.tsv looks like:
#
# --env TASK_ID --input INPUT_PATH --output OUTPUT_PATH
# TASK_1 gs://bucket/path/NA06986.chromY...bam gs://bucket/path/1/*.md5
# TASK_2 gs://bucket/path/NA06986.chrom21...bam gs://bucket/path/2/*.md5
# TASK_3 gs://bucket/path/NA06986.chrom18...bam gs://bucket/path/3/*.md5

# pylint: disable=common_typos_disable
# pilot3_exon_targetted_GRCh37_bams raises a "common typos" warning: "targetted"

_IO_TASKS_META = textwrap.dedent("""
  create-time: {}
""".format(CREATE_TIME_STR) + """
  dsub-version: 0.1.4
  envs:
    TEST_NAME: io_tasks
  inputs:
    POPULATION_FILE: gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.superpopulations.tsv
  job-id: script_io_--dsubuser--171201-083727-449848
  job-name: script_io_test.sh
  logging: gs://b/dsub/sh/local/io_tasks/io_tasks/logging/
  outputs:
    OUTPUT_POPULATION_FILE: gs://b/dsub/sh/local/io_tasks/output/*
  task-ids: 1-3
  tasks:
  - envs:
      TASK_ID: TASK_3
    inputs:
      INPUT_PATH: gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam
    logging-path: gs://b/dsub/sh/local/io_tasks/logging/script_io_--dsubuser--171201-083727-449848.3.log
    outputs:
      OUTPUT_PATH: gs://b/dsub/sh/local/io_tasks/output/3/*.md5
    task-id: '3'
  user-id: dsubuser
""")

_IO_TASKS_JOB_DESCRIPTOR = job_model.JobDescriptor(
    job_metadata={
        'dsub-version': '0.1.4',
        'job-id': 'script_io_--dsubuser--171201-083727-449848',
        'job-name': 'script_io_test.sh',
        'task-ids': '1-3',
        'user-id': 'dsubuser',
        'create-time': CREATE_TIME
    },
    job_resources=job_model.Resources(
        logging='gs://b/dsub/sh/local/io_tasks/io_tasks/logging/'),
    job_params={
        'envs': {job_model.EnvParam('TEST_NAME', 'io_tasks'),},
        'labels': set(),
        'inputs': {
            job_model.InputFileParam(
                'POPULATION_FILE',
                'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.superpopulations.tsv',  # pylint: disable=line-too-long
                recursive=False),
        },
        'outputs': {
            job_model.OutputFileParam(
                'OUTPUT_POPULATION_FILE',
                'gs://b/dsub/sh/local/io_tasks/output/*',
                recursive=False),
        },
        'input-recursives': set(),
        'output-recursives': set(),
        'mounts': set(),
    },
    task_descriptors=[
        job_model.TaskDescriptor(
            task_metadata={'task-id': '3'},
            task_resources=job_model.Resources(
                logging_path=
                'gs://b/dsub/sh/local/io_tasks/logging/script_io_--dsubuser--171201-083727-449848.3.log'  # pylint: disable=line-too-long
            ),
            task_params={
                'envs': {job_model.EnvParam('TASK_ID', 'TASK_3'),},
                'labels': set(),
                'inputs': {
                    job_model.InputFileParam(
                        'INPUT_PATH',
                        'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment/NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam',  # pylint: disable=line-too-long
                        recursive=False),
                },
                'outputs': {
                    job_model.OutputFileParam(
                        'OUTPUT_PATH',
                        'gs://b/dsub/sh/local/io_tasks/output/3/*.md5',
                        recursive=False),
                },
                'input-recursives': set(),
                'output-recursives': set(),
            })
    ])

# The following test data is from the e2e_io_tasks.sh test.

# dsub \
#   --image "google/cloud-sdk:latest" \
#   --script "${SCRIPT_DIR}/script_io_recursive.sh" \
#   --env FILE_CONTENTS="${FILE_CONTENTS}" \
#   --input INPUT_PATH_SHALLOW="${INPUTS}/shallow/*" \
#   --input-recursive INPUT_PATH_DEEP="${INPUTS}/deep/" \
#   --output OUTPUT_PATH_SHALLOW="${OUTPUTS}/shallow/*" \
#   --output-recursive OUTPUT_PATH_DEEP="${OUTPUTS}/deep/"

_IO_RECURSIVE_META = textwrap.dedent("""
  create-time: {}
""".format(CREATE_TIME_STR) + """
  dsub-version: 0.1.4
  envs:
    FILE_CONTENTS: Test file contents
  input-recursives:
    INPUT_PATH_DEEP: gs://b/dsub/sh/local/io_recursive/input/deep/
  inputs:
    INPUT_PATH_SHALLOW: gs://b/dsub/sh/local/io_recursive/input/shallow/*
  job-id: script_io_--dsubuser--171201-135702-356673
  job-name: script_io_recursive.sh
  logging: gs://b/dsub/sh/local/io_recursive/logging/io_recursive.log
  output-recursives:
    OUTPUT_PATH_DEEP: gs://b/dsub/sh/local/io_recursive/output/deep/
  outputs:
    OUTPUT_PATH_SHALLOW: gs://b/dsub/sh/local/io_recursive/output/shallow/*
  tasks:
  - logging-path: gs://b/dsub/sh/local/io_recursive/logging/io_recursive.log
    task-id: null
  user-id: dsubuser
""")

_IO_RECURSIVE_JOB_DESCRIPTOR = job_model.JobDescriptor(
    job_metadata={
        'dsub-version': '0.1.4',
        'job-id': 'script_io_--dsubuser--171201-135702-356673',
        'job-name': 'script_io_recursive.sh',
        'user-id': 'dsubuser',
        'create-time': CREATE_TIME
    },
    job_resources=job_model.Resources(
        logging='gs://b/dsub/sh/local/io_recursive/logging/io_recursive.log'),
    job_params={
        'envs': {job_model.EnvParam('FILE_CONTENTS', 'Test file contents'),},
        'labels': set(),
        'inputs': {
            job_model.InputFileParam(
                'INPUT_PATH_SHALLOW',
                'gs://b/dsub/sh/local/io_recursive/input/shallow/*',
                recursive=False),
        },
        'input-recursives': {
            job_model.InputFileParam(
                'INPUT_PATH_DEEP',
                'gs://b/dsub/sh/local/io_recursive/input/deep/',
                recursive=True),
        },
        'outputs': {
            job_model.OutputFileParam(
                'OUTPUT_PATH_SHALLOW',
                'gs://b/dsub/sh/local/io_recursive/output/shallow/*',
                recursive=False),
        },
        'output-recursives': {
            job_model.OutputFileParam(
                'OUTPUT_PATH_DEEP',
                'gs://b/dsub/sh/local/io_recursive/output/deep/',
                recursive=True),
        },
        'mounts': set(),
    },
    task_descriptors=[
        job_model.TaskDescriptor(
            task_metadata={'task-id': None},
            task_resources=job_model.Resources(
                logging_path='gs://b/dsub/sh/local/io_recursive/logging/io_recursive.log'
            ),
            task_params={
                'envs': set(),
                'labels': set(),
                'inputs': set(),
                'outputs': set(),
                'input-recursives': set(),
                'output-recursives': set(),
            })
    ])

# The following test data is from the e2e_io_tasks.sh test.

# dsub \
#   --tasks "e2e_label.tsv" \
#   --label batch=hello-world \
#   --command "echo 'hello world'
#
# where io_tasks.tsv looks like:
#
# --label item-number
# 1
# 2

_LABELS_META = textwrap.dedent("""
  create-time: {}
""".format(CREATE_TIME_STR) + """
  dsub-version: 0.1.4
  job-id: echo--dsubuser--171201-142229-050417
  job-name: echo
  labels:
    batch: hello-world
  logging: gs://b/dsub/sh/local/labels/logging/labels.log
  task-ids: 1-2
  tasks:
  - labels:
      item-number: '2'
    logging-path: gs://b/dsub/sh/local/labels/logging/labels.2.log
    task-id: '2'
  user-id: dsubuser
""")

_LABELS_JOB_DESCRIPTOR = job_model.JobDescriptor(
    job_metadata={
        'dsub-version': '0.1.4',
        'job-id': 'echo--dsubuser--171201-142229-050417',
        'job-name': 'echo',
        'task-ids': '1-2',
        'user-id': 'dsubuser',
        'create-time': CREATE_TIME
    },
    job_resources=job_model.Resources(
        logging='gs://b/dsub/sh/local/labels/logging/labels.log'),
    job_params={
        'envs': set(),
        'labels': {job_model.LabelParam('batch', 'hello-world')},
        'inputs': set(),
        'outputs': set(),
        'input-recursives': set(),
        'output-recursives': set(),
        'mounts': set(),
    },
    task_descriptors=[
        job_model.TaskDescriptor(
            task_metadata={'task-id': '2'},
            task_resources=job_model.Resources(
                logging_path='gs://b/dsub/sh/local/labels/logging/labels.2.log'
            ),
            task_params={
                'envs': set(),
                'labels': {job_model.LabelParam('item-number', '2')},
                'inputs': set(),
                'outputs': set(),
                'input-recursives': set(),
                'output-recursives': set(),
            })
    ])


class JobDescriptorTest(unittest.TestCase):

  def assert_job_metadata_equal(self, actual, expected):
    self.assertEqual(actual, expected)

  def assert_job_resources_equal(self, actual, expected):
    self.assertEqual(actual, expected)

  def assert_job_params_equal(self, actual, expected):
    self.assertEqual(actual, expected)

  def assert_job_descriptors_equal(self, actual, expected):
    self.assert_job_metadata_equal(actual.job_metadata, expected.job_metadata)
    self.assert_job_resources_equal(actual.job_resources,
                                    expected.job_resources)
    self.assert_job_params_equal(actual.job_params, expected.job_params)

    for a, e in zip(actual.task_descriptors, expected.task_descriptors):
      self.assert_job_metadata_equal(a.task_metadata, e.task_metadata)
      self.assert_job_resources_equal(a.task_resources, e.task_resources)
      self.assert_job_params_equal(a.task_params, e.task_params)

  @parameterized.parameterized.expand([
      ('env_list', _ENV_LIST_META, _ENV_LIST_JOB_DESCRIPTOR),
      ('io_tasks', _IO_TASKS_META, _IO_TASKS_JOB_DESCRIPTOR),
      ('io_recursive', _IO_RECURSIVE_META, _IO_RECURSIVE_JOB_DESCRIPTOR),
      ('labels', _LABELS_META, _LABELS_JOB_DESCRIPTOR),
  ])
  def test_from_yaml(self, unused_name, yaml_string, expected):
    actual = job_model.JobDescriptor.from_yaml(yaml_string)
    self.assert_job_descriptors_equal(actual, expected)


class TestJobModelDocs(unittest.TestCase):

  def test_doctest(self):
    result = doctest.testmod(job_model, report=True)
    self.assertEqual(0, result.failed)


if __name__ == '__main__':
  unittest.main()
