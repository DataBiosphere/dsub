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
"""Error classes for dsub specific operations."""


class JobError(Exception):
  """Exception containing error information of one or more jobs."""

  def __init__(self, message, error_list):
    super(JobError, self).__init__(message)
    self.message = message
    self.error_list = error_list


class PredecessorJobFailureError(JobError):
  pass


class JobExecutionError(JobError):
  pass
