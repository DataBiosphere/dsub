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

"""Test calling dsub from python with TSV-file inputs.

This test is a copy of the e2e_io_tasks.sh test with additional checks
on the objects returned by dsub.call().
"""
from __future__ import print_function

import os
import sys

# Because this may be invoked from another directory (treated as a library) or
# invoked localy (treated as a binary) both import styles need to be supported.
# pylint: disable=g-import-not-at-top
try:
  from . import test_setup_e2e as test
  from . import test_util
except SystemError:
  import test_setup_e2e as test
  import test_util

POPULATION_FILE = 'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/20131219.superpopulations.tsv'
POPULATION_MD5 = '68a73f849b82071afe11888bac1aa8a7'

# Disable the linter that flags several of the following files for a typo.
# common_typos_disable
INPUT_BAMS_PATH = 'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/technical/pilot3_exon_targetted_GRCh37_bams/data/NA06986/alignment'

INPUT_BAMS = (INPUT_BAMS_PATH +
              '/NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam',
              INPUT_BAMS_PATH +
              '/NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam',
              INPUT_BAMS_PATH +
              '/NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam')

INPUT_BAMS_MD5 = ('4afb9b8908959dbd4e2d5c54bf254c93',
                  '0dc006ed39ddad2790034ca497631234',
                  '36e37a0dab5926dbf5a1b8afc0cdac8b')

# Build up an array of lines for the TSV.
with open(test.TASKS_FILE, 'w') as f:
  f.write('--env TASK_ID\t--input INPUT_PATH\t--output OUTPUT_PATH\n')
  for i in range(len(INPUT_BAMS)):
    f.write('TASK_{task}\t{input}\t{output_path}/{task}/*.md5\n'.format(
        task=i + 1, input=INPUT_BAMS[i], output_path=test.OUTPUTS))

print('Launching pipeline...')

# pyformat: disable
launched_job = test.run_dsub([
    '--script', '%s/script_io_test.sh' % test.TEST_DIR,
    '--tasks', test.TASKS_FILE,
    '--env', 'TEST_NAME=%s' % test.TEST_NAME,
    '--input', 'POPULATION_FILE_PATH=%s' % POPULATION_FILE,
    '--output', 'OUTPUT_POPULATION_FILE=%s/*' % test.OUTPUTS,
    '--wait'])
# pyformat: enable

# Sanity check launched_jobs - should have a single record with 3 tasks
if not launched_job:
  print('No launched jobs returned', file=sys.stderr)
  sys.exit(1)

if not launched_job.get('job-id'):
  print('Launched job contains no job id', file=sys.stderr)
  sys.exit(1)

if not launched_job.get('user-id'):
  print('Launched job contains no user-id.', file=sys.stderr)
  sys.exit(1)

if len(launched_job.get('task-id', [])) != 3:
  print('Launched job does not contain 3 tasks.', file=sys.stderr)
  print(launched_job.get('task-id'), file=sys.stderr)
  sys.exit(1)

print('Launched job: %s' % launched_job['job-id'])
for task in launched_job['task-id']:
  print('  task: %s' % task)

print('\nChecking output...')

TASKS_COUNT = len(INPUT_BAMS)

for i in range(TASKS_COUNT):
  INPUT_BAM = INPUT_BAMS[i]
  RESULT_EXPECTED = INPUT_BAMS_MD5[i]

  OUTPUT_PATH = test_util.get_field_from_tsv(
      test.TASKS_FILE, '--input INPUT_PATH', r'^%s$' % INPUT_BAM,
      '--output OUTPUT_PATH')
  OUTPUT_FILE = '%s/%s.md5' % (OUTPUT_PATH[:-len('/*.md5')],
                               os.path.basename(INPUT_BAM))
  RESULT = test_util.gsutil_cat(OUTPUT_FILE)

  if not test_util.diff(RESULT_EXPECTED.strip(), RESULT.strip()):
    print('Output file does not match expected')
    sys.exit(1)

  print('\nOutput file matches expected:')
  print('*****************************')
  print('RESULT')
  print('*****************************')

# Check that the population file got copied for each of the tasks
RESULT_EXPECTED = POPULATION_MD5
for i in range(TASKS_COUNT):
  OUTPUT_FILE = '%s/TASK_%s.md5' % (test.OUTPUTS, (i + 1))
  RESULT = test_util.gsutil_cat(OUTPUT_FILE)

  if not test_util.diff(RESULT_EXPECTED.strip(), RESULT.strip()):
    print('Output file does not match expected')
    sys.exit(1)

  print('\nOutput file matches expected:')
  print('*****************************')
  print('RESULT')
  print('*****************************')

print('SUCCESS')
