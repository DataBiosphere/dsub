#!/bin/bash

set -o errexit
set -o nounset

if [[ "$#" -gt 0 ]]; then
  cat <<EOF
USAGE:
  $0

Sets up a virtualenv named dsub_libs in the current working directory and runs
the unit tests. If the virtualenv already exists, will install the dependent
dsub libraries into it.
EOF
  exit 0
fi

# We need to be in test's parent folder, so adjust if necessary.

cd "$(dirname "$0")/.."

echo ""
echo "Setting up."
echo ""

set +o nounset
source test/setup_virtualenv
set -o nounset

echo ""
echo "Starting tests."
echo ""

# TODO: once this works reliably, add the e2e tests

if ! python -m unittest discover -s test/unit/; then
  echo "A test in test/unit/ failed."
  exit 1
fi

echo "Tests passed."
