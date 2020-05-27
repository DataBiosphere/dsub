# Lint as: python2, python3
# Copyright 2018 Verily Life Sciences Inc. All Rights Reserved.
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
"""Utility routines for constructing a Google Genomics Pipelines v2 API request.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from . import google_v2_versions

STATUS_FILTER_MAP = {
    'RUNNING': 'done = false',
    'CANCELED': 'error = 1',
    'FAILURE': 'error > 1',
    'SUCCESS': 'error = 0',
}

_API_VERSION = None


def set_api_version(api_version):
  assert api_version in (google_v2_versions.V2ALPHA1, google_v2_versions.V2BETA)

  global _API_VERSION
  _API_VERSION = api_version


def label_filter(label_key, label_value):
  """Return a valid label filter for operations.list()."""
  return 'labels."{}" = "{}"'.format(label_key, label_value)


def create_time_filter(create_time, comparator):
  """Return a valid createTime filter for operations.list()."""
  return 'createTime {} "{}"'.format(comparator, create_time)


def get_create_time(op):
  """Return the create time string of the operation."""
  return op.get('metadata', {}).get('createTime')


def get_start_time(op):
  """Return the start time string of the operation."""
  return op.get('metadata', {}).get('startTime')


def get_end_time(op):
  """Return the end time string of the operation."""
  return op.get('metadata', {}).get('endTime')


def get_metadata_type(op):
  """Return the internal metadata type of the operation."""
  return op.get('metadata', {}).get('@type')


def get_error(op):
  """Return the error structure for the operation."""
  return op.get('error')


def is_done(op):
  """Return whether the operation has been marked done."""
  return op.get('done', False)


def is_success(op):
  """Return whether the operation has completed successfully."""
  return is_done(op) and ('error' not in op)


def is_canceled(op):
  """Return whether the operation was canceled by the user."""
  error = get_error(op)
  return error and error.get('code', 0) == 1


def is_failed(op):
  """Return whether the operation has failed."""
  error = get_error(op)
  return error and error.get('code', 0) > 1


def get_labels(op):
  """Return the operation's array of labels."""
  return op.get('metadata', {}).get('labels', {})


def get_label(op, name):
  """Return the value for the specified label."""
  return get_labels(op).get(name)


def get_actions(op):
  """Return the operation's array of actions."""
  return op.get('metadata', {}).get('pipeline').get('actions', [])


def get_action_by_id(op, action_id):
  """Return the operation's array of actions."""
  actions = get_actions(op)
  if actions and 1 <= action_id < len(actions):
    return actions[action_id - 1]


def get_action_by_name(op, name):
  """Return the value for the specified action."""
  actions = get_actions(op)
  for action in actions:
    if _API_VERSION == google_v2_versions.V2ALPHA1:
      if action.get('name') == name:
        return action

    elif _API_VERSION == google_v2_versions.V2BETA:
      if action.get('containerName') == name:
        return action

    else:
      assert False, 'Unexpected version: {}'.format(_API_VERSION)


def get_action_environment(op, name):
  """Return the environment for the operation."""
  action = get_action_by_name(op, name)
  if action:
    return action.get('environment')


def get_action_image(op, name):
  """Return the image for the operation."""
  action = get_action_by_name(op, name)
  if action:
    return action.get('imageUri')


def get_events(op):
  """Return the array of events for the operation."""
  return op.get('metadata', {}).get('events', [])


def get_last_event(op):
  """Return the last event (if any) for the operation."""
  events = get_events(op)
  if events:
    return events[0]
  return None


def get_failed_events(op):
  """Return the events (if any) with a non-zero exitStatus."""
  events = get_events(op)
  if events:
    return [
        e for e in events if int(e.get('details', {}).get('exitStatus', 0)) != 0
    ]
  return None


def get_event_of_type(op, event_type):
  """Return all events of a particular type."""
  events = get_events(op)
  if not events:
    return None

  return [e for e in events if e.get('details', {}).get('@type') == event_type]


def _get_worker_assigned_events(op):
  """Return all "Worker Assigned" events."""

  events = get_events(op)
  if not events:
    return None

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    return [
        e for e in events if e.get('details', {}).get('@type') ==
        'type.googleapis.com/google.genomics.v2alpha1.WorkerAssignedEvent'
    ]

  elif _API_VERSION == google_v2_versions.V2BETA:
    return [e for e in events if 'workerAssigned' in e]

  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)


def get_worker_assigned_event_details(op):
  """Return the detail portion of the most recent "worker assigned" event."""

  events = _get_worker_assigned_events(op)
  if not events:
    return None

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    return events[0].get('details', {})

  elif _API_VERSION == google_v2_versions.V2BETA:
    return events[0].get('workerAssigned', {})

  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)


def get_last_update(op):
  """Return the most recent timestamp in the operation."""
  last_update = get_end_time(op)

  if not last_update:
    last_event = get_last_event(op)
    if last_event:
      last_update = last_event['timestamp']

  if not last_update:
    last_update = get_create_time(op)

  return last_update


def get_resources(op):
  """Return the operation's resource."""
  return op.get('metadata', {}).get('pipeline').get('resources', {})


def get_vm_network_name(vm):
  """Return the name of the network from the virtualMachine."""

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    return vm.get('network', {}).get('name')

  elif _API_VERSION == google_v2_versions.V2BETA:
    return vm.get('network', {}).get('network')

  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)


def is_pipeline(op):
  """Check that an operation is a genomics pipeline run.

  An operation is a Genomics Pipeline run if the request metadata's @type
  is "type.googleapis.com/google.genomics.v2alpha1.Metadata".

  Args:
    op: a pipelines operation.

  Returns:
    Boolean, true if the operation is a RunPipelineRequest.
  """

  if _API_VERSION == google_v2_versions.V2ALPHA1:
    return get_metadata_type(
        op) == 'type.googleapis.com/google.genomics.v2alpha1.Metadata'

  elif _API_VERSION == google_v2_versions.V2BETA:
    return get_metadata_type(
        op) == 'type.googleapis.com/google.cloud.lifesciences.v2beta.Metadata'

  else:
    assert False, 'Unexpected version: {}'.format(_API_VERSION)


def is_dsub_operation(op):
  """Determine if a pipelines operation is a dsub request.

  We don't have a rigorous way to identify an operation as being submitted
  by dsub. Our best option is to check for certain fields that have always
  been part of dsub operations.

  - labels: job-id, job-name, and user-id have always existed. The dsub-version
            label has always existed for the google-v2 provider.

  Args:
    op: a pipelines operation.

  Returns:
    Boolean, true if the pipeline run was generated by dsub.
  """
  if not is_pipeline(op):
    return False

  for name in ['dsub-version', 'job-id', 'job-name', 'user-id']:
    if not get_label(op, name):
      return False

  return True


if __name__ == '__main__':
  pass
