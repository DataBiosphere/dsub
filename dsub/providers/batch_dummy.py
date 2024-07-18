# Copyright 2022 Verily Life Sciences Inc. All Rights Reserved.
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
"""Dummy module to hold types for Batch. To be removed when library exists."""

# TODO: This whole file goes away once the official client library
# is made available.


# pylint: disable=invalid-name
class types(object):
  """Dummy docstring."""
  Job = None
  TaskGroup = None
  Volume = None
  TaskSpec = None
  Environment = None
  ServiceAccount = None

  class task(object):

    class Runnable(object):
      Container = None

  class AllocationPolicy(object):
    InstancePolicyOrTemplate = None
    InstancePolicy = None
    AttachedDisk = None
    Disk = None
    NetworkPolicy = None
    Accelerator = None
    LocationPolicy = None
    ProvisioningModel = None

  class LogsPolicy(object):
    Destination = None


# pylint: enable=invalid-name
