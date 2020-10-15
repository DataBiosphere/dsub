# Copyright 2016 Google Inc. All Rights Reserved.
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
"""Fake time, for testing."""


class FakeTime(object):
  """Fake time, for testing."""

  def __init__(self, chronology):
    """The chronology is a generator that updates the world.

    Yield how many seconds of fake time elapse. Think of each yield as a
    "sleep". We read the very first value immediately, so that the world can be
    initialized in the chronology.

    Args:
      chronology: a generator that updates the state of the world and then
      sleeps via yielding.
    """
    self._chronology = chronology
    # current fake time
    self._now = 0
    # time at which to wake up the chronology
    self._next = next(self._chronology)

  def sleep(self, seconds):
    """Sleep for this many fictitious seconds.

    The world will update accordingly.

    Args:
      seconds: how many seconds to pretend sleep for.

    Raises:
      IOError: for negative delays (matches time.sleep).
      BaseException: at the end of times, to detect infinite loop bugs
    and the like.
    """
    if seconds < 0:
      # Chosen to match the behavior of time.sleep.
      raise IOError("Invalid argument")
    target = self._now + seconds
    if target < self._next:
      self._now = target
      return
    self._now = target
    for step in self._chronology:
      self._next += step
      if self._next > target:
        return
    raise BaseException("end of times")

  def now(self):
    """Returns the current fake time."""
    return self._now
