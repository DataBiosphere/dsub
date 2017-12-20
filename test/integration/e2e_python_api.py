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

import os
import sys
import time

from dsub.commands import dstat
from dsub.commands import dsub
from dsub.lib import job_util
from dsub.lib import param_util
from dsub.lib import resources
from dsub.providers import google
from dsub.providers import local

import test_setup
import test_setup_e2e as test


def get_dsub_provider():
  if test.DSUB_PROVIDER == 'local':
    return local.LocalJobProvider(resources)
  elif test.DSUB_PROVIDER == 'google':
    return google.GoogleJobProvider(False, False, test.PROJECT_ID)
  else:
    print >> sys.stderr, 'Invalid provider specified.'
    sys.exit(1)


def dsub_start_job(command,
                   name=None,
                   envs=None,
                   labels=None,
                   inputs=None,
                   inputs_recursive=None,
                   outputs=None,
                   outputs_recursive=None,
                   wait=False):

  envs = envs or {}
  labels = labels or {}
  inputs = inputs or {}
  inputs_recursive = inputs_recursive or {}
  outputs = outputs or {}
  outputs_recursive = outputs_recursive or {}

  labels['test-token'] = test_setup.TEST_TOKEN

  logging = param_util.build_logging_param(test.LOGGING)
  job_resources = job_util.JobResources(
      image='ubuntu', logging=logging, zones=['us-central1-*'])

  env_data = {param_util.EnvParam(k, v) for (k, v) in envs.items()}
  label_data = {param_util.LabelParam(k, v) for (k, v) in labels.items()}

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

  job_data = {
      'envs': env_data,
      'inputs': input_data,
      'outputs': output_data,
      'labels': label_data,
  }
  all_task_data = [{
      'envs': env_data,
      'labels': label_data,
      'inputs': input_data,
      'outputs': output_data,
  }]

  return dsub.run(
      get_dsub_provider(),
      job_resources,
      job_data,
      all_task_data,
      name=name,
      command=command,
      wait=wait,
      disable_warning=True)


def dstat_get_jobs(statuses=None,
                   job_ids=None,
                   task_ids=None,
                   labels=None,
                   create_time_min=None,
                   create_time_max=None):
  statuses = statuses or {'*'}
  labels = labels or {}
  labels['test-token'] = test_setup.TEST_TOKEN
  labels_set = {param_util.LabelParam(k, v) for (k, v) in labels.items()}

  return dstat.dstat_job_producer(
      provider=get_dsub_provider(),
      statuses=statuses,
      job_ids=job_ids,
      task_ids=task_ids,
      labels=labels_set,
      create_time_min=create_time_min,
      create_time_max=create_time_max,
      full_output=True).next()


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
  print message
  sys.exit(1)


if not os.environ.get('CHECK_RESULTS_ONLY'):
  print 'Launching pipelines...'
  job1 = dsub_start_job('echo FIRST', name='job1')
  time.sleep(1)
  job2 = dsub_start_job('echo SECOND', name='job2')
  time.sleep(1)
  job3 = dsub_start_job('echo THIRD', name='job3')

  first_job = dstat_get_jobs(job_ids={job1['job-id']})[0]
  second_job = dstat_get_jobs(job_ids={job2['job-id']})[0]
  third_job = dstat_get_jobs(job_ids={job3['job-id']})[0]

  first_ct = first_job['create-time']
  second_ct = second_job['create-time']
  third_ct = third_job['create-time']

  if dstat_get_job_names(create_time_min=first_ct) != ['job3', 'job2', 'job1']:
    exit_with_message('Failed to get all jobs by create_time_min.')
  if dstat_get_job_names(create_time_min=second_ct) != ['job3', 'job2']:
    exit_with_message('Failed to get second and third jobs by create_time_min.')
  if dstat_get_job_names(create_time_min=third_ct) != ['job3']:
    exit_with_message('Failed to get the third job by create_time_min')
  if dstat_get_job_names(create_time_max=first_ct) != ['job1']:
    exit_with_message('Failed to get the first job by create_time_max')
  if dstat_get_job_names(create_time_max=second_ct) != ['job2', 'job1']:
    exit_with_message(
        'Failed to get the first and second jobs by create_time_max.')
  if dstat_get_job_names(create_time_max=third_ct) != ['job3', 'job2', 'job1']:
    exit_with_message('Failed to get the all jobs by create_time_max.')
  if dstat_get_job_names(
      create_time_min=first_ct, create_time_max=first_ct) != ['job1']:
    exit_with_message('Failed to get the first job by create time range.')
  if dstat_get_job_names(
      create_time_min=second_ct, create_time_max=second_ct) != ['job2']:
    exit_with_message('Failed to get the second job by create time range.')
  if dstat_get_job_names(
      create_time_min=third_ct, create_time_max=third_ct) != ['job3']:
    exit_with_message('Failed to get the third job by create time range.')
  if dstat_get_job_names(
      create_time_min=first_ct, create_time_max=second_ct) != ['job2', 'job1']:
    exit_with_message(
        'Failed to get the first and second jobs by create time range.')
  if dstat_get_job_names(
      create_time_min=second_ct, create_time_max=third_ct) != ['job3', 'job2']:
    exit_with_message(
        'Failed to get the second and third jobs by create time range.')
  if dstat_get_job_names(
      create_time_min=first_ct,
      create_time_max=third_ct) != ['job3', 'job2', 'job1']:
    exit_with_message('Failed to get all jobs by create time range.')
