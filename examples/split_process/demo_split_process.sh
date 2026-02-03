#!/bin/bash

#
# This is an example master script that runs a simple two step workflow:
#   - Run a dsub job to split an input file into shards
#   - Run a `--tasks` job over each shard in parallel
#
# Usage:
#   ./demo_split_process.sh <inputfile> <local_path_or_gcs>
#
# example:
#   WORKSPACE=gs://mybucket/someprefix
#   ./demo_split_process.sh input.txt "${WORKSPACE}"
#   gcloud storage ls "${WORKSPACE}/output/"
#   gcloud storage rm "${WORKSPACE}/temp/*"
#
# You need dsub, docker, and gcloud storage installed.
# Change WORKSPACE to point to a bucket you have write permission to.
#
# Since this uses the local provider, you can set WORKSPACE to a local path,
# e.g. WORKSPACE=/tmp/demo_split_process
# To run on Google Cloud, delete the "--provider local \" line.

set -o errexit
set -o nounset

INPUT="${1}"
WORKSPACE="${2}"

SHARD_COUNT=4

echo "Processing input file: ${INPUT}"

# Step one: process the input file into multiple splits.
# They are written in ${WORKSPACE}/temp/1_split/
dsub \
  --provider local \
  --env SHARD_COUNT="${SHARD_COUNT}" \
  --input IN="${INPUT}" \
  --output OUT="${WORKSPACE}/temp/1_split/*" \
  --logging "${WORKSPACE}/logs/1_split.log" \
  --script "remote_scripts/split_into_shards.sh" \
  --wait


# Step two: process each split.
# We use --tasks for that, so we first create the tsv.
readonly TASKS_FILE="${TMPDIR:-/tmp}/dsub_demo.tsv"
rm -f "${TASKS_FILE}"

echo -e "--env SHARD\t--input IN\t--output OUT" >> "${TASKS_FILE}";
for ((SHARD = 1; SHARD <= SHARD_COUNT; SHARD++)); do
  echo -e "${SHARD}\t${WORKSPACE}/temp/1_split/split-${SHARD}-of-${SHARD_COUNT}\t${WORKSPACE}/output/shard-${SHARD}-of-${SHARD_COUNT}" >> "${TASKS_FILE}";
done

# Run dsub over the TASKS_FILE. Output saved to ${WORKSPACE}/output/
dsub \
  --provider local \
  --tasks "${TASKS_FILE}" \
  --script "remote_scripts/process_shard.sh" \
  --logging "${WORKSPACE}/logs/2_process.log" \
  --wait

# --wait will block until all the tasks are done.
# So at this point the output's ready.

echo "Demo complete."
echo "Output in ${WORKSPACE}/output/"
