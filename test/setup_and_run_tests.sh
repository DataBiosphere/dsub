#!/bin/bash

set -o errexit
set -o nounset

if [[ "${1:-}" == "--help" ]]; then
  cat <<EOF
USAGE:
  $0 [unit|e2e|pythonunit]

Sets up a virtualenv named dsub_libs in the current working directory and runs
the specified tests (or all if none is specified).
If the virtualenv already exists, will install the dependent dsub libraries into it.
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

echo "Your test bucket is: gs://${USER}-dsub-test"

if ! test/run_tests.sh "$@"; then
  echo "test/run_tests $* failed."
  exit 1
fi

echo "Tests passed."
