"""setuptools-based installation script.

File is based on this template: https://github.com/pypa/sampleproject
"""

import os
import sys
import unittest
import warnings
# Always prefer setuptools over distutils
from setuptools import find_packages
from setuptools import setup

if sys.version_info[0] != 2:
  warnings.warn('Python 3.x support is experimental.')


def unittest_suite():
  """Get test suite (Python unit tests only)."""
  test_loader = unittest.TestLoader()
  test_suite = test_loader.discover('test/unit', pattern='test_*.py')
  return test_suite


def get_dsub_version():
  """Get the dsub version out of the _dsub_version.py source file.

  Setup.py should not import dsub version from dsub directly since ambiguity in
  import order could lead to an old version of dsub setting the version number.
  Parsing the file directly is simpler than using import tools (whose interface
  varies between python 2.7, 3.4, and 3.5).

  Returns:
    string of dsub version.

  Raises:
    ValueError: if the version is not found.
  """
  filename = os.path.join(os.path.dirname(__file__), 'dsub/_dsub_version.py')
  with open(filename, 'r') as versionfile:
    for line in versionfile:
      if line.startswith('DSUB_VERSION ='):
        # Get the version then strip whitespace and quote characters.
        version = line.partition('=')[2]
        return version.strip().strip('\'"')
  raise ValueError('Could not find version.')


setup(
    name='dsub',

    # Versions should comply with PEP440.
    version=get_dsub_version(),
    description=('A command-line tool that makes it easy to submit and run'
                 ' batch scripts in the cloud'),

    # The project's main homepage.
    url='https://github.com/DataBiosphere/dsub',

    # Author details
    author='Verily',
    # Choose your license
    license='Apache',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 1 - Planning',
        # 'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: System :: Distributed Computing',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

    # What does your project relate to?
    keywords='cloud bioinformatics',

    # Packages to distribute.
    packages=find_packages(),
    include_package_data=True,

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.
    install_requires=[
        # dependencies for dsub, ddel, dstat
        'google-api-python-client',
        'oauth2client',
        'python-dateutil',
        'pytz',
        'pyyaml==3.12',
        'retrying',
        'six',
        'tabulate',

        # dependencies for test code
        'parameterized',
    ],

    # Define a test suite for Python unittests only.
    test_suite='setup.unittest_suite',

    # Provide executable scripts - these will be added to the user's path.
    entry_points={
        'console_scripts': [
            'dsub=dsub.commands.dsub:main',
            'dstat=dsub.commands.dstat:main',
            'ddel=dsub.commands.ddel:main',
        ],
    },
)
