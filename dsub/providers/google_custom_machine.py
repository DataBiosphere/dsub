# Copyright 2024 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility class for creating custom machine types.

See documentation on restrictions at
https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type
"""
import math

from ..lib import job_model


class GoogleCustomMachine(object):
  """Utility class for creating custom machine types."""

  # See documentation on restrictions at
  # https://cloud.google.com/compute/docs/instances/creating-instance-with-custom-machine-type
  _MEMORY_MULTIPLE = 256  # The total memory must be a multiple of 256 MB
  _MIN_MEMORY_PER_CPU_IN_GB = 0.9  # in GB per vCPU
  _MAX_MEMORY_PER_CPU_IN_GB = 6.5  # in GB per vCPU
  _MB_PER_GB = 1024  # 1 GB = 1024 MB

  # Memory is input in GB, but we'd like to do our calculations in MB
  _MIN_MEMORY_PER_CPU = _MIN_MEMORY_PER_CPU_IN_GB * _MB_PER_GB
  _MAX_MEMORY_PER_CPU = _MAX_MEMORY_PER_CPU_IN_GB * _MB_PER_GB

  @staticmethod
  def _validate_cores(cores):
    """Make sure cores is either one or even."""
    if cores == 1 or cores % 2 == 0:
      return cores
    else:
      return cores + 1

  @staticmethod
  def _validate_ram(ram_in_mb):
    """Rounds ram up to the nearest multiple of _MEMORY_MULTIPLE."""
    return int(
        GoogleCustomMachine._MEMORY_MULTIPLE
        * math.ceil(ram_in_mb / GoogleCustomMachine._MEMORY_MULTIPLE)
    )

  @classmethod
  def build_machine_type(cls, min_cores, min_ram):
    """Returns a custom machine type string."""
    min_cores = min_cores or job_model.DEFAULT_MIN_CORES
    min_ram = min_ram or job_model.DEFAULT_MIN_RAM

    # First, min_ram is given in GB. Convert to MB.
    min_ram *= GoogleCustomMachine._MB_PER_GB

    # Only machine types with 1 vCPU or an even number of vCPUs can be created.
    cores = cls._validate_cores(min_cores)
    # The total memory of the instance must be a multiple of 256 MB.
    ram = cls._validate_ram(min_ram)

    # Memory must be between 0.9 GB per vCPU, up to 6.5 GB per vCPU.
    memory_to_cpu_ratio = ram / cores

    if memory_to_cpu_ratio < GoogleCustomMachine._MIN_MEMORY_PER_CPU:
      # If we're under the ratio, top up the memory.
      adjusted_ram = GoogleCustomMachine._MIN_MEMORY_PER_CPU * cores
      ram = cls._validate_ram(adjusted_ram)

    elif memory_to_cpu_ratio > GoogleCustomMachine._MAX_MEMORY_PER_CPU:
      # If we're over the ratio, top up the CPU.
      adjusted_cores = math.ceil(ram / GoogleCustomMachine._MAX_MEMORY_PER_CPU)
      cores = cls._validate_cores(adjusted_cores)

    else:
      # Ratio is within the restrictions - no adjustments needed.
      pass

    return 'custom-{}-{}'.format(int(cores), int(ram))
