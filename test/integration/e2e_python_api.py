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
"""Test calling dsub and dstat directly via the python interface."""

from __future__ import print_function

import os
import sys
import time

from dsub.commands import dstat
from dsub.commands import dsub
from dsub.lib import job_model
from dsub.lib import param_util
from dsub.lib import resources
from dsub.providers import google_cls_v2
from dsub.providers import google_v2
from dsub.providers import local
import six

# Because this may be invoked from another directory (treated as a library) or
# invoked localy (treated as a binary) both import styles need to be supported.
# pylint: disable=g-import-not-at-top
try:
  from . import test_setup
  from . import test_setup_e2e as test
except SystemError:
  import test_setup
  import test_setup_e2e as test


def get_dsub_provider():
  """Return the appropriate google_base.JobProvider instance."""
  if test.DSUB_PROVIDER == 'local':
    return local.LocalJobProvider(resources)
  elif test.DSUB_PROVIDER == 'google-cls-v2':
    return google_cls_v2.GoogleCLSV2JobProvider(False, test.PROJECT_ID,
                                                'us-central1')
  elif test.DSUB_PROVIDER == 'google-v2':
    return google_v2.GoogleV2JobProvider(False, test.PROJECT_ID)
  else:
    print('Invalid provider specified.', file=sys.stderr)
    sys.exit(1)


def dsub_start_job(command,
                   job_name=None,
                   envs=None,
                   labels=None,
                   inputs=None,
                   inputs_recursive=None,
                   outputs=None,
                   outputs_recursive=None,
                   wait=False):
  """Build up test parameters and call dsub.run()."""

  envs = envs or {}
  labels = labels or {}
  inputs = inputs or {}
  inputs_recursive = inputs_recursive or {}
  outputs = outputs or {}
  outputs_recursive = outputs_recursive or {}

  labels['test-token'] = test_setup.TEST_TOKEN
  labels['test-name'] = test_setup.TEST_NAME

  logging = param_util.build_logging_param(test.LOGGING)
  job_resources = job_model.Resources(
      image='ubuntu', logging=logging, zones=['us-central1-*'])

  env_data = {job_model.EnvParam(k, v) for (k, v) in envs.items()}
  label_data = {job_model.LabelParam(k, v) for (k, v) in labels.items()}

  input_file_param_util = param_util.InputFileParamUtil('input')
  input_data = set()
  for (recursive, items) in ((False, inputs.items()),
                             (True, inputs_recursive.items())):
    for (name, value) in items:
      name = input_file_param_util.get_variable_name(name)
      input_data.add(input_file_param_util.make_param(name, value, recursive))

  output_file_param_util = param_util.OutputFileParamUtil('output')
  output_data = set()
  for (recursive, items) in ((False, outputs.items()),
                             (True, outputs_recursive.items())):
    for (name, value) in items:
      name = output_file_param_util.get_variable_name(name)
      output_data.add(output_file_param_util.make_param(name, value, recursive))

  job_params = {
      'envs': env_data,
      'inputs': input_data,
      'outputs': output_data,
      'labels': label_data,
  }
  task_descriptors = [
      job_model.TaskDescriptor({
          'task-id': None
      }, {
          'envs': set(),
          'labels': set(),
          'inputs': set(),
          'outputs': set(),
      }, job_model.Resources())
  ]

  return dsub.run(
      get_dsub_provider(),
      job_resources,
      job_params,
      task_descriptors,
      name=job_name,
      command=command,
      wait=wait,
      disable_warning=True)


def dstat_get_jobs(statuses=None,
                   job_ids=None,
                   task_ids=None,
                   labels=None,
                   create_time_min=None,
                   create_time_max=None):
  """Build up test parameters and call dstat.dstat_job_producer()."""

  statuses = statuses or {'*'}
  labels = labels or {}
  labels['test-token'] = test_setup.TEST_TOKEN
  labels['test-name'] = test_setup.TEST_NAME
  labels_set = {job_model.LabelParam(k, v) for (k, v) in labels.items()}

  return six.advance_iterator(
      dstat.dstat_job_producer(
          provider=get_dsub_provider(),
          statuses=statuses,
          job_ids=job_ids,
          task_ids=task_ids,
          labels=labels_set,
          create_time_min=create_time_min,
          create_time_max=create_time_max,
          full_output=True))


def dstat_get_job_names(statuses=None,
                        job_ids=None,
                        task_ids=None,
                        labels=None,
                        create_time_min=None,
                        create_time_max=None):
  return [
      j['job-name']
      for j in dstat_get_jobs(statuses, job_ids, task_ids, labels,
                              create_time_min, create_time_max)
  ]


def exit_with_message(message):
  print(message)
  sys.exit(1)


def dstat_check_job_names(err_message,
                          target,
                          statuses=None,
                          job_ids=None,
                          task_ids=None,
                          labels=None,
                          create_time_min=None,
                          create_time_max=None):
  """Call the dstat API, get the job names and verify they are as expected."""

  actual = dstat_get_job_names(statuses, job_ids, task_ids, labels,
                               create_time_min, create_time_max)

  if target != actual:
    print('FAILED: ' + err_message)
    print('actual = {}'.format(actual))
    print('target = {}'.format(target))
    exit_with_message('Exiting')


if not os.environ.get('CHECK_RESULTS_ONLY'):
  print('Launching jobs...')
  job1 = dsub_start_job('echo FIRST', job_name='job1')
  time.sleep(1)
  job2 = dsub_start_job('echo SECOND', job_name='job2')
  time.sleep(1)
  job3 = dsub_start_job('echo THIRD', job_name='job3')

  print('Getting jobs')
  first_job = dstat_get_jobs(job_ids={job1['job-id']})[0]
  second_job = dstat_get_jobs(job_ids={job2['job-id']})[0]
  third_job = dstat_get_jobs(job_ids={job3['job-id']})[0]

  print('Checking jobs')
  first_ct = first_job['create-time']
  second_ct = second_job['create-time']
  third_ct = third_job['create-time']

  dstat_check_job_names(
      'Get jobs by create_time_min = {}.'.format(first_ct),
      ['job3', 'job2', 'job1'],
      create_time_min=first_ct)

  dstat_check_job_names(
      'Get jobs by create_time_min = {}.'.format(second_ct), ['job3', 'job2'],
      create_time_min=second_ct)

  dstat_check_job_names(
      'Get jobs by create_time_min = {}.'.format(third_ct), ['job3'],
      create_time_min=third_ct)

  dstat_check_job_names(
      'Get jobs by create_time_max = {}.'.format(first_ct), ['job1'],
      create_time_max=first_ct)

  dstat_check_job_names(
      'Get jobs by create_time_max = {}.'.format(second_ct), ['job2', 'job1'],
      create_time_max=second_ct)

  dstat_check_job_names(
      'Get jobs by create_time_max = {}.'.format(third_ct),
      ['job3', 'job2', 'job1'],
      create_time_max=third_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          first_ct, first_ct), ['job1'],
      create_time_min=first_ct,
      create_time_max=first_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          second_ct, second_ct), ['job2'],
      create_time_min=second_ct,
      create_time_max=second_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          third_ct, third_ct), ['job3'],
      create_time_min=third_ct,
      create_time_max=third_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          first_ct, second_ct), ['job2', 'job1'],
      create_time_min=first_ct,
      create_time_max=second_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          second_ct, third_ct), ['job3', 'job2'],
      create_time_min=second_ct,
      create_time_max=third_ct)

  dstat_check_job_names(
      'Get jobs by range: create_time_min = {}, create_time_max = {}.'.format(
          first_ct, third_ct), ['job3', 'job2', 'job1'],
      create_time_min=first_ct,
      create_time_max=third_ct)

print('SUCCESS')
