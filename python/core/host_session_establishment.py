# Copyright (c) 2021 Ian Craggs
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    https://www.eclipse.org/legal/epl-2.0/
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Ian Craggs - initial API and implementation and/or initial documentation

import time

import paho.mqtt.client as mqtt


class ControlClient(mqtt.Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.published = False

    def on_message(self, client, userdata, msg):
        if msg.topic == "SPARKPLUG_TCK/RESULT":
            print("*** Result ***", msg.payload)

    def on_connect(self, client, userdata, flags, rc):
        print(f"Control client connected with result code {rc}")
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("SPARKPLUG_TCK/#")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("Control client subscribed")
        client.publish(
            "SPARKPLUG_TCK/TEST_CONTROL",
            f"NEW host SessionEstablishment {host_application_id}",
            qos=1,
        )

    def control_on_publish(self, client, userdata, mid):
        print("Control client published")
        self.published = True


class TestClient(mqtt.Client):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.published = False

    def on_connect(self, client, userdata, flags, rc):
        print(f"Test client connected with result code {rc}")
        client.subscribe("spAv1.0/#")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("Test client subscribed")
        client.publish(f"STATE/{host_application_id}", "ONLINE", qos=1)

    def on_publish(self, client, userdata, mid):
        print("Test client published")
        self.published = True


if __name__ == "__main__":
    broker = "localhost"
    port = 1883
    host_application_id = "HOSTAPPID"

    control_client = ControlClient("sparkplug_control")
    control_client.connect(broker, port)
    control_client.loop_start()

    # wait for publish to complete
    while not control_client.published:
        time.sleep(0.1)

    test_client = TestClient("clientid", clean_session=True)
    test_client.will_set(f"STATE/{host_application_id}", "OFFLINE", qos=1, retain=True)
    test_client.connect(broker, port)
    test_client.loop_start()

    while not test_client.published:
        time.sleep(0.1)

    test_client.loop_stop()

    control_client.published = False
    control_client.publish("SPARKPLUG_TCK/TEST_CONTROL", "END TEST")
    while not control_client.published:
        time.sleep(0.1)

    control_client.loop_stop()
