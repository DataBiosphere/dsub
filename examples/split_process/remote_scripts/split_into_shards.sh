#!/bin/bash

# This script is meant to be run from dsub.
# It performs a toy splitting the input into shards.
# The input environment variables are:
# OUT - output pattern
# SHARD_COUNT - how many shards to make

set -o errexit
set -o nounset

OUT_DIR=$(dirname "${OUT}")

# Split the file by lines where the first line in shard n is line n,
# followed by each <SHARD_COUNT> line thereafter.
#
# So if there are 3 shards, then:
#  shard 1 gets lines 1, 4, 7, etc.
#  shard 2 gets lines 2, 5, 8, etc.
#  shard 3 gets lines 3, 6, 9, etc.
for ((SHARD = 1; SHARD <= ${SHARD_COUNT}; SHARD++)); do
  awk "(NR%${SHARD_COUNT})==${SHARD} "'{ print $0 }' "${IN}" > "${OUT_DIR}/split-${SHARD}-of-${SHARD_COUNT}"
done
