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
"""Utility classes for merging multiple query results in a sorted manner."""

import collections
import Queue


class SortedGeneratorIterator(collections.Iterator):
  """Sorted iterator wrapping an arbitrary number of sorted generators.

  Implemented with a PriorityQueue of generators to allow O(ln(N)) sorting of
  each stream.

  Note: Proper use of this class requires that every generator passed to
  add_generator yields objects in the same sort order as the initialized lambda
  key. If this is not the case the resulting items will not be sorted.
  """

  def __init__(self, maxsize=0, key=None):
    self._key = key if key else lambda item: item
    self._queue = Queue.PriorityQueue(maxsize=maxsize)

  def __iter__(self):
    return self

  def empty(self):
    return self._queue.empty()

  def next(self):
    if self.empty():
      raise StopIteration

    _, val, generator = self._queue.get()
    self.add_generator(generator)
    return val

  def add_generator(self, generator):
    try:
      next_val = generator.next()
    except StopIteration:
      return False

    value_tuple = (self._key(next_val), next_val, generator)
    self._queue.put(value_tuple)
    return True
