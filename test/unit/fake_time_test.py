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
"""Unit tests for fake_time.
"""

import unittest
import fake_time


class TestFakeTime(unittest.TestCase):
  """Unit tests for fake_time."""

  def establish_chronology(self, chronology):
    self._fake_time = fake_time.FakeTime(chronology)
    self.sleep = self._fake_time.sleep

  def nothing_happens_for_10_seconds(self):
    yield 10

  def red_light(self):
    while True:
      self.light = "red"
      yield 3
      self.light = "green"
      yield 3
      self.light = "yellow"
      yield 1

  def test_short_sleep(self):
    self.establish_chronology(self.nothing_happens_for_10_seconds())
    self.sleep(9)
    self.assertEqual(self._fake_time.now(), 9)
    self.sleep(0)
    self.assertEqual(self._fake_time.now(), 9)

  def test_negative_sleep(self):
    self.establish_chronology(self.nothing_happens_for_10_seconds())
    with self.assertRaises(IOError):
      self.sleep(-1)

  def test_sleep_past_end_of_time(self):
    self.establish_chronology(self.nothing_happens_for_10_seconds())
    with self.assertRaises(BaseException):
      self.sleep(10)

  def test_sleeps_add_together(self):
    self.establish_chronology(self.red_light())
    self.assertEqual(self.light, "red")
    self.sleep(3)
    self.assertEqual(self.light, "green")
    self.sleep(1)
    self.assertEqual(self.light, "green")
    self.sleep(1)
    self.assertEqual(self.light, "green")
    self.sleep(1)
    self.assertEqual(self.light, "yellow")
    self.sleep(1)
    self.assertEqual(self.light, "red")

  def test_sleep_through_more_than_one_step(self):
    self.establish_chronology(self.red_light())
    self.sleep(6)
    self.assertEqual(self.light, "yellow")

  def test_halways_into_a_step(self):
    self.establish_chronology(self.red_light())
    self.sleep(5)
    self.assertEqual(self.light, "green")
    self.assertEqual(self._fake_time.now(), 5)
    self.sleep(4)
    self.assertEqual(self.light, "red")
    self.assertEqual(self._fake_time.now(), 9)


if __name__ == "__main__":
  unittest.main()
