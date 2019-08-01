# Copyright 2019-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from xossynchronizer.model_policies.policy import Policy
from cord_workflow_controller_client.probe import Probe
import os
import sys

sync_path = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
sys.path.append(sync_path)

controller_url = 'http://controller:3030'
probe = None
retry_conn_max = 3
connected = False
retry = 0


def connect():
    if not connected:
        global probe, connected, retry
        if retry > retry_conn_max:
            probe = None
            connected = False
        else:
            try:
                probe = Probe()
                probe.connect(controller_url)
                connected = True
                retry = 0
            except Exception:
                probe = None
                connected = False
                retry += 1


def emit_helper(instance, event_type):
    topic = 'datamodel.%s' % instance.model_name
    message = {
        'event_type': event_type
    }

    if not connected:
        connect()

    if connected:
        if 'log' in instance:
            instance.log.info('Emitting an event (%s - %s)...' % (topic, message))

        if probe:
            probe.emit_event(topic, message)

        if 'log' in instance:
            instance.log.info('Emitted an event (%s - %s)...' % (topic, message))
    else:
        if 'log' in instance:
            instance.log.info('Skip emitting an event (%s - %s)...' % (topic, message))


class CORDWorkflowModelEventProbe_ATTSI(Policy):
    # TODO: NEED TO ALLOW MULTI-SUBSCRIPTION OF MODEL UPDATE EVENTS AT A LOWER LEVEL
    # TO ELIMINATE CREATION OF A CLASS PER MODEL
    model_name = "AttWorkflowDriverServiceInstance"

    def handle_create(self, instance):
        emit_helper(self, 'create')

    def handle_update(self, instance):
        emit_helper(self, 'update')

    def handle_delete(self, instance):
        emit_helper(self, 'delete')


class CORDWorkflowModelEventProbe_ATTWL(Policy):
    model_name = "AttWorkflowDriverWhiteListEntry"

    def handle_create(self, instance):
        emit_helper(self, 'create')

    def handle_update(self, instance):
        emit_helper(self, 'update')

    def handle_delete(self, instance):
        emit_helper(self, 'delete')
