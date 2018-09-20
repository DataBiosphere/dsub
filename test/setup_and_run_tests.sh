#!/bin/bash

set -o errexit
set -o nounset

if [[ "${1:-}" == "--help" ]]; then
  cat <<EOF
USAGE:
  $0 [2.7|3.5] [unit|e2e|pythonunit]

Sets up a virtualenv in the current working directory and runs the specified
tests (or all if none is specified).

A Python version number (2|3|2.N|3.N) may be passed as the first argument and
the virtualenv will be created with this version of Python. If no version is
specified, "2" will be used by default.
EOF
  exit 0
fi

# Get the python version.
if grep -qP "^[23]\.?[567]?$" <<< "${1}" ; then
    PYTHON_VERSION="${1}"
    shift
else
    PYTHON_VERSION="2"
fi


# We need to be in test's parent folder, so adjust if necessary.

cd "$(dirname "$0")/.."

echo ""
echo "Setting up."
echo ""

set +o nounset
source test/setup_virtualenv "${PYTHON_VERSION}"
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
