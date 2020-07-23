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

SHELL := /bin/bash

VIRTUALENV := dsub_env

PYTHON_VERSION := $(shell python3 --version 2>&1)
PYTHON_LIST := $(wordlist 2,4,$(subst ., ,$(PYTHON_VERSION)))
PYTHON_MAJOR_VERSION := $(word 1,${PYTHON_LIST})
PYTHON_MINOR_VERSION := $(word 2,${PYTHON_LIST})

PIP_VERSION := $(shell pip --version 2>/dev/null)
PIP_LIST := $(wordlist 2,4,$(subst ., ,$(PIP_VERSION)))
PIP_MAJOR_VERSION := $(word 1, ${PIP_LIST})

all: checkversions

install: checkversions virtualenv

checkversions:
ifndef PYTHON_VERSION
	$(error python3 executable not found; either update the path or install Python 3)
endif
ifneq "$(PYTHON_MAJOR_VERSION)" "3"
	$(error Bad python version $(PYTHON_LIST); install Python 3)
endif
ifndef PIP_VERSION
	$(error pip executable not found; either update the path or install pip)
endif
ifeq ($(PIP_MAJOR_VERSION),$(filter $(PIP_MAJOR_VERSION),"1" "2" "3" "4" "5" "6"))
	$(error Bad pip version $(PIP_LIST); install >= 7.0.0)
endif
	@echo All prechecks pass. Ready to 'make install'.

virtualenv:
	python3 -m venv dsub_libs
	source dsub_libs/bin/activate && \
		python setup.py install
	@echo Installed. You can now run the programs in bin/.

clean:
	rm -rf dsub_libs
