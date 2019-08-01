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

import json
from xossynchronizer.event_steps.eventstep import EventStep
from cord_workflow_controller_client.probe import Probe


class CORDWorkflowEventProbe(EventStep):
    # topics = ["onu.events", "dhcp.events", "authentication.events"]
    topics = ['*.events']
    technology = 'kafka'
    controller_url = 'http://controller:3030'
    retry_conn_max = 3

    def __init__(self, *args, **kwargs):
        super(CORDWorkflowEventProbe, self).__init__(*args, **kwargs)

        self.connected = False
        self.retry = 0
        self.connect()

    def connect(self):
        if not self.connected:
            if self.retry > self.retry_conn_max:
                self.log.info(
                    'Could not connect to Workflow Controller (%s)...' %
                    self.controller_url
                )
                self.probe = None
                self.connected = False
            else:
                try:
                    self.log.info(
                        'Connecting to Workflow Controller (%s)...' %
                        self.controller_url
                    )

                    self.probe = Probe(logger=self.log)
                    self.probe.connect(self.controller_url)
                    self.connected = True
                    self.retry = 0
                except Exception:
                    self.probe = None
                    self.connected = False
                    self.retry += 1

    def process_event(self, event):
        if not self.connected:
            self.connect()

        if self.connected:
            topic = event.topic
            # event is in json format
            message = json.loads(event.value)

            self.log.info('Emitting an event (%s - %s)...' % (topic, message))
            self.probe.emit_event(topic, message)
            self.log.info('Emitted an event (%s - %s)...' % (topic, message))
        else:
            self.log.info('Skip emitting an event (%s - %s)...' % (topic, message))
