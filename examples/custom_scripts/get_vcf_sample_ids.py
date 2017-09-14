#!/usr/bin/env python # pylint: disable=g-unknown-interpreter

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
"""Extract sample IDs from a VCF file.
"""

import gzip
import os
import sys

INPUT_VCF = os.environ['INPUT_VCF']
OUTPUT_FILE = os.environ['OUTPUT_FILE']

# If the input VCF is compressed, then open it with the gzip library
if INPUT_VCF.endswith('.gz'):
  infile = gzip.open(INPUT_VCF, 'r')
else:
  infile = open(INPUT_VCF, 'r')

## Extract the "#CHROM" header
for line in infile:
  if line.startswith('#CHROM'):
    chrom = line.rstrip()
    infile.close()
    break

if not chrom:
  print >> sys.stderr, 'Failed to find #CHROM header'
  sys.exit(1)

# Split the #CHROM header into separate fields
fields = chrom.split('\t')

# Emit everything after the "FORMAT" field, one field per line
with open(OUTPUT_FILE, 'w') as outfile:
  outfile.write('\n'.join(fields[fields.index('FORMAT') + 1:]))
  outfile.write('\n')
