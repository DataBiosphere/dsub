#!/bin/bash

# This script is meant to be run from dsub.
# It pretends to process an input shard.
# The input environment variables are:
# IN - input file
# OUT - output file

set -o errexit
set -o nounset

cp "${IN}" "${OUT}"
echo "Shard file $(basename "${IN}") contains $(wc -c "${IN}" | awk '{print $1}') words." >> "${OUT}"
