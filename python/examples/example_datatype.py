#!/usr/bin/python

# Copyright (c) 2014, 2018 Cirrus Link Solutions and others
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0.
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#   Cirrus Link Solutions - initial implementation

import random
import string
import sys
import time

import paho.mqtt.client as mqtt

from core.sparkplug_b import (
    SparkplugDevice,
    SparkplugNode,
    create_metric,
)
from core.sparkplug_b_pb2 import DataType, Payload


class ExampleDevice(SparkplugDevice):
    def publish_device_birth(self, client, seq, topic_base):
        """Publish the DBIRTH certificate."""
        print("Publishing Device Birth")

        # Get the payload
        payload = self.get_device_birth_payload(seq)
        payload.metrics.extend(
            [
                create_metric("Int8_Min", None, DataType.Int8, -128),
                create_metric("Int8_Max", None, DataType.Int8, 127),
                create_metric("Int16_Min", None, DataType.Int16, -32768),
                create_metric("Int16_Max", None, DataType.Int16, 32767),
                create_metric("Int32_Min", None, DataType.Int32, -2147483648),
                create_metric("Int32_Max", None, DataType.Int32, 2147483647),
                create_metric("Int64_Min", None, DataType.Int64, -9223372036854775808),
                create_metric("Int64_Max", None, DataType.Int64, 9223372036854775807),
                create_metric("UInt8_Min", None, DataType.UInt8, 0),
                create_metric("UInt8_Max", None, DataType.UInt8, 255),
                create_metric("UInt16_Min", None, DataType.UInt16, 0),
                create_metric("UInt16_Max", None, DataType.UInt16, 64535),
                create_metric("UInt32_Min", None, DataType.UInt32, 0),
                create_metric("UInt32_Max", None, DataType.UInt32, 4294967295),
                create_metric("UInt64_Min", None, DataType.UInt64, 0),
                create_metric(
                    "UInt64_Max", None, DataType.UInt64, 18446744073709551615
                ),
            ]
        )

        # Publish the initial data with the Device BIRTH certificate
        total_byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"{topic_base}/{self.id}",
            total_byte_array,
            0,
            False,
        )

    def publish_ddata(self, client, seq, topic_base):
        payload = self.get_ddata_payload(seq)
        payload.metrics.extend(
            [
                # Add some random data to the inputs
                create_metric(
                    None,
                    None,
                    DataType.String,
                    "".join(random.choice(string.ascii_lowercase) for _ in range(12)),
                ),
                # Note this data we're setting to STALE via the propertyset as an example
                create_metric(
                    None,
                    None,
                    DataType.Boolean,
                    random.choice([True, False]),
                    properties=Payload.PropertySet(
                        keys=["Quality"],
                        values=[
                            Payload.PropertyValue(type=DataType.Int32, int_value=500)
                        ],
                    ),
                ),
            ]
        )

        # Publish a message data
        byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"{topic_base}/{self.id}",
            byte_array,
            0,
            False,
        )

    def handle_dcmd(self, client, payload, topic_base):
        for metric in payload.metrics:
            if metric.name == "output/Device Metric2":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Int16 because of how we declated it in the DBIRTH
                new_value = metric.int_value
                print(f"CMD message for output/Device Metric2 - New Value: {new_value}")

                # Create the DDATA payload - Use the alias because this isn't the DBIRTH
                payload = self.get_ddata_payload()
                payload.metrics.append(
                    create_metric(None, None, DataType.Int16, new_value)
                )

                # Publish a message data
                byte_array = bytearray(payload.SerializeToString())
                client.publish(f"{topic_base}/{self.id}", byte_array, 0, False)
            elif metric.name == "output/Device Metric3":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Boolean because of how we declated it in the DBIRTH
                new_value = metric.boolean_value
                print(f"CMD message for output/Device Metric3 - New Value: {new_value}")

                # Create the DDATA payload - use the alias because this isn't the DBIRTH
                payload = self.get_ddata_payload()
                payload.metrics.append(
                    create_metric(None, None, DataType.Boolean, new_value)
                )

                # Publish a message data
                byte_array = bytearray(payload.SerializeToString())
                client.publish(f"{topic_base}/{self.id}", byte_array, 0, False)
            else:
                print(f"Unknown command: {metric.name}")


class ExampleNode(SparkplugNode):
    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            print(f"Connected with result code {rc}")
        else:
            print(f"Failed to connect with result code {rc}")
            sys.exit()

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(f"spBv1.0/{self.group_id}/NCMD/{self.id}/#")
        client.subscribe(f"spBv1.0/{self.group_id}/DCMD/{self.id}/#")

    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server."""
        print(f"Message arrived: {msg.topic}")
        tokens = msg.topic.split("/")

        if (
            tokens[0] == "spBv1.0"
            and tokens[1] == self.group_id
            and tokens[2] == "NCMD"
            and tokens[3] == self.id
        ):
            inbound_payload = Payload()
            inbound_payload.ParseFromString(msg.payload)
            self.handle_ncmd(client, inbound_payload)
        elif (
            tokens[0] == "spBv1.0"
            and tokens[1] == self.group_id
            and tokens[2] == "DCMD"
            and tokens[3] == self.id
        ):
            device_id = tokens[4]
            inbound_payload = Payload()
            inbound_payload.ParseFromString(msg.payload)
            device = next((d for d in self._devices if d.id == device_id), None)
            if device:
                device.handle_dcmd(
                    client,
                    inbound_payload,
                    f"spBv1.0/{self.group_id}/DDATA/{self.id}",
                )
        else:
            print("Unknown command...")

        print("Done publishing")

    def handle_ncmd(self, _, payload):
        for metric in payload.metrics:
            if metric.name == "Node Control/Next Server":
                # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                # disconnect from the current MQTT server and connect to the next MQTT server in the
                # list of available servers.  This is used for clients that have a pool of MQTT servers
                # to connect to.
                print("'Node Control/Next Server' is not implemented in this example")
            elif metric.name == "Node Control/Rebirth":
                # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                # application if it receives an NDATA or DDATA with a metric that was not published in the
                # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                # its original NBIRTH and DBIRTH messages.
                self.publish_birth()
            elif metric.name == "Node Control/Reboot":
                # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                # This can be used for devices that need a full application reset via a soft reboot.
                # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                # messages.
                self.publish_birth()
            else:
                print(f"Unknown command: {metric.name}")

    def publish_birth(self, client):
        """Publish the BIRTH certificates."""
        self.publish_node_birth(client)
        for device in self._devices:
            device.publish_device_birth(
                client,
                self.get_seq_num(),
                f"spBv1.0/{self.group_id}/DBIRTH/{self.id}",
            )

    def publish_node_birth(self, client):
        """Publish the NBIRTH certificate."""
        print("Publishing Node Birth")

        # Create the node birth payload
        payload = self.get_node_birth_payload()
        payload.metrics.extend(
            [
                # Set up the Node Controls
                create_metric(
                    "Node Control/Next Server", None, DataType.Boolean, False
                ),
                create_metric("Node Control/Rebirth", None, DataType.Boolean, False),
                create_metric("Node Control/Reboot", None, DataType.Boolean, False),
            ]
        )

        # Publish the node birth certificate
        byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"spBv1.0/{self.group_id}/NBIRTH/{self.id}", byte_array, 0, False
        )


def main(
    server_url,
    group_id,
    node_name,
    device_name,
    username,
    password,
):
    print("Starting main application")

    device = ExampleDevice(device_name)
    node = ExampleNode(node_name, group_id, [device])

    # Start of main program - Set up the MQTT client connection
    client = mqtt.Client(server_url, 1883, 60)

    client.on_connect = node.on_connect
    client.on_message = node.on_message

    # Create the node death payload
    death_payload = node.get_node_death_payload()
    death_byte_array = bytearray(death_payload.SerializeToString())
    death_topic = f"spBv1.0/{node.group_id}/NDEATH/{node.id}"
    client.will_set(death_topic, death_byte_array, 0, False)

    client.username_pw_set(username, password)
    client.connect(server_url, 1883, 60)

    # Short delay to allow connect callback to occur
    time.sleep(0.1)
    client.loop()

    # Publish the birth certificates
    node.publish_birth(client)

    while True:
        # Periodically publish some new data
        device.publish_ddata(
            client, node.get_seq_num(), f"spBv1.0/{node.group_id}/DDATA/{node.id}"
        )

        # Sit and wait for inbound or outbound events
        for _ in range(5):
            time.sleep(0.1)
            client.loop()


if __name__ == "__main__":
    main(
        server_url="localhost",
        group_id="Sparkplug B Devices",
        node_name="Python Edge Node 1",
        device_name="Emulated Device",
        username="admin",
        password="changeme",
    )
