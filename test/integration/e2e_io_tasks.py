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

import os
import sys

import test_setup_e2e as test
import test_util

if not os.environ.get('CHECK_RESULTS_ONLY'):
  print 'Launching pipeline...'

  # pyformat: disable
  launched_job = test.run_dsub([
      '--script', '%s/script_io_test.sh' % test.TEST_DIR,
      '--tasks', test.TASKS_FILE,
      '--wait'])
  # pyformat: enable

  # Sanity check launched_jobs - should have a single record with 3 tasks
  if not launched_job:
    print >> sys.stderr, 'No launched jobs returned'
    sys.exit(1)

  if not launched_job.get('job-id'):
    print >> sys.stderr, 'Launched job contains no job id'
    sys.exit(1)

  if not launched_job.get('user-id'):
    print >> sys.stderr, 'Launched job contains no user-id.'
    sys.exit(1)

  if len(launched_job.get('task-id', [])) != 3:
    print >> sys.stderr, 'Launched job does not contain 3 tasks.'
    print >> sys.stderr, launched_job.get('task-id')
    sys.exit(1)

  print 'Launched job: %s' % launched_job['job-id']
  for task in launched_job['task-id']:
    print '  task: %s' % task

print
print 'Checking output...'

INPUT_BAMS = ('NA06986.chromY.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam',
              'NA06986.chrom21.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam',
              'NA06986.chrom18.ILLUMINA.bwa.CEU.exon_targetted.20100311.bam')

RESULTS_EXPECTED = ('4afb9b8908959dbd4e2d5c54bf254c93',
                    '0dc006ed39ddad2790034ca497631234',
                    '36e37a0dab5926dbf5a1b8afc0cdac8b')

for i in range(len(INPUT_BAMS)):
  INPUT_BAM = INPUT_BAMS[i]
  RESULT_EXPECTED = RESULTS_EXPECTED[i]

  OUTPUT_PATH = test_util.get_field_from_tsv(
      test.TASKS_FILE, '--input INPUT_PATH', r'^.*/%s$' % INPUT_BAM,
      '--output OUTPUT_PATH')
  OUTPUT_FILE = '%s/%s.md5' % (OUTPUT_PATH[:-len('/*.md5')], INPUT_BAM)
  RESULT = test_util.gsutil_cat(OUTPUT_FILE)

  if not test_util.diff(RESULT_EXPECTED.strip(), RESULT.strip()):
    print 'Output file does not match expected'
    sys.exit(1)

  print
  print 'Output file matches expected:'
  print '*****************************'
  print 'RESULT'
  print '*****************************'

print 'SUCCESS'
