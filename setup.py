"""setuptools-based installation script.

File is based on this template: https://github.com/pypa/sampleproject
"""

import unittest
# Always prefer setuptools over distutils
from setuptools import find_packages
from setuptools import setup


def unittest_suite():
  """Get test suite (Python unit tests only)."""
  test_loader = unittest.TestLoader()
  test_suite = test_loader.discover('test/unit', pattern='test_*.py')
  return test_suite

DESCRIPTION = ('A command-line tool that makes it easy to submit and run'
               ' batch scripts in the cloud')
PROJECT_URL = 'https://github.com/googlegenomics/dsub'
KEYWORDS = 'cloud bioinformatics'

# TODO(e4p): Restore full classifier list and project metadata. The metadata
#              was trimmed prior to the initial pypi push. The metadata will be
#              restored when we push the first dsub release.
setup(
    name='dsub',

    # Versions should comply with PEP440.
    version='0.0.0',
    description='dsub',

    # The project's main homepage.
    url='',

    # Author details
    author='Google',
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
        # 'Topic :: Scientific/Engineering :: Bio-Informatics',
        # 'Topic :: Scientific/Engineering :: Information Analysis',
        # 'Topic :: System :: Distributed Computing',

        # Pick your license as you wish (should match "license" above)
        # 'License :: OSI Approved :: Apache License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
    ],

    # What does your project relate to?
    keywords='',
    # Packages to distribute.
    packages=find_packages(),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.
    install_requires=[
        # dependencies for dsub, ddel, dstat
        'google-api-python-client',
        'oauth2client',
        'python-dateutil',
        'pytz',
        'pyyaml',
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
    },)
