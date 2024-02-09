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
    MetricDataType,
    ParameterDataType,
    add_metric,
    get_ddata_payload,
    get_device_birth_payload,
    get_node_birth_payload,
    get_node_death_payload,
)
from core.sparkplug_b_pb2 import Payload

# Application Variables
server_url = "localhost"
my_group_id = "Sparkplug B Devices"
my_node_name = "Python Edge Node 1"
my_device_name = "Emulated Device"
publish_period = 5000
my_username = "admin"
my_passowrd = "changeme"


def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    if rc == 0:
        print("Connected with result code " + str(rc))
    else:
        print("Failed to connect with result code " + str(rc))
        sys.exit()

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(f"spBv1.0/{my_group_id}/NCMD/{my_node_name}/#")
    client.subscribe(f"spBv1.0/{my_group_id}/DCMD/{my_node_name}/#")


def on_message(client, userdata, msg):
    """Callback for when a PUBLISH message is received from the server."""
    print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if (
        tokens[0] == "spBv1.0"
        and tokens[1] == my_group_id
        and (tokens[2] == "NCMD" or tokens[2] == "DCMD")
        and tokens[3] == my_node_name
    ):
        inbound_payload = Payload()
        inbound_payload.ParseFromString(msg.payload)
        for metric in inbound_payload.metrics:
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
                publish_birth()
            elif metric.name == "Node Control/Reboot":
                # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                # This can be used for devices that need a full application reset via a soft reboot.
                # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                # messages.
                publish_birth()
            elif metric.name == "output/Device Metric2":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Int16 because of how we declated it in the DBIRTH
                new_value = metric.int_value
                print(f"CMD message for output/Device Metric2 - New Value: {new_value}")

                # Create the DDATA payload - Use the alias because this isn't the DBIRTH
                payload = get_ddata_payload()
                add_metric(payload, None, None, MetricDataType.Int16, new_value)

                # Publish a message data
                byte_array = bytearray(payload.SerializeToString())
                client.publish(
                    f"spBv1.0/{my_group_id}/DDATA/{my_node_name}/{my_device_name}",
                    byte_array,
                    0,
                    False,
                )
            elif metric.name == "output/Device Metric3":
                # This is a metric we declared in our DBIRTH message and we're emulating an output.
                # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                # value.  If this were a real output we'd write to the output and then read it back
                # before publishing a DDATA message.

                # We know this is an Boolean because of how we declated it in the DBIRTH
                new_value = metric.boolean_value
                print(
                    "CMD message for output/Device Metric3 - New Value: %r" % new_value
                )

                # Create the DDATA payload - use the alias because this isn't the DBIRTH
                payload = get_ddata_payload()
                add_metric(payload, None, None, MetricDataType.Boolean, new_value)

                # Publish a message data
                byte_array = bytearray(payload.SerializeToString())
                client.publish(
                    f"spBv1.0/{my_group_id}/DDATA/{my_node_name}/{my_device_name}",
                    byte_array,
                    0,
                    False,
                )
            else:
                print("Unknown command: " + metric.name)
    else:
        print("Unknown command...")

    print("Done publishing")


def publish_birth():
    """Publish the BIRTH certificates."""
    publish_node_birth()
    publish_device_birth()


def publish_node_birth():
    """Publish the NBIRTH certificate."""
    print("Publishing Node Birth")

    # Create the node birth payload
    payload = get_node_birth_payload()

    # Set up the Node Controls
    add_metric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    add_metric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    add_metric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

    # Publish the node birth certificate
    byte_array = bytearray(payload.SerializeToString())
    client.publish(
        f"spBv1.0/{my_group_id}/NBIRTH/{my_node_name}", byte_array, 0, False
    )


#
def publish_device_birth():
    """Publish the DBIRTH certificate."""
    print("Publishing Device Birth")

    # Get the payload
    payload = get_device_birth_payload()

    # Add some device metrics
    add_metric(payload, "Int8_Min", None, MetricDataType.Int8, -128)
    add_metric(payload, "Int8_Max", None, MetricDataType.Int8, 127)
    add_metric(payload, "Int16_Min", None, MetricDataType.Int16, -32768)
    add_metric(payload, "Int16_Max", None, MetricDataType.Int16, 32767)
    add_metric(payload, "Int32_Min", None, MetricDataType.Int32, -2147483648)
    add_metric(payload, "Int32_Max", None, MetricDataType.Int32, 2147483647)
    add_metric(payload, "Int64_Min", None, MetricDataType.Int64, -9223372036854775808)
    add_metric(payload, "Int64_Max", None, MetricDataType.Int64, 9223372036854775807)

    add_metric(payload, "UInt8_Min", None, MetricDataType.UInt8, 0)
    add_metric(payload, "UInt8_Max", None, MetricDataType.UInt8, 255)
    add_metric(payload, "UInt16_Min", None, MetricDataType.UInt16, 0)
    add_metric(payload, "UInt16_Max", None, MetricDataType.UInt16, 64535)
    add_metric(payload, "UInt32_Min", None, MetricDataType.UInt32, 0)
    add_metric(payload, "UInt32_Max", None, MetricDataType.UInt32, 4294967295)
    add_metric(payload, "UInt64_Min", None, MetricDataType.UInt64, 0)
    add_metric(payload, "UInt64_Max", None, MetricDataType.UInt64, 18446744073709551615)

    # Publish the initial data with the Device BIRTH certificate
    total_byte_array = bytearray(payload.SerializeToString())
    client.publish(
        f"spBv1.0/{my_group_id}/DBIRTH/{my_node_name}/{my_device_name}",
        total_byte_array,
        0,
        False,
    )


# Main Application
print("Starting main application")

# Create the node death payload
death_payload = get_node_death_payload()

# Start of main program - Set up the MQTT client connection
client = mqtt.Client(server_url, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(my_username, my_passowrd)
death_byte_array = bytearray(death_payload.SerializeToString())
client.will_set(
    f"spBv1.0/{my_group_id}/NDEATH/{my_node_name}", death_byte_array, 0, False
)
client.connect(server_url, 1883, 60)

# Short delay to allow connect callback to occur
time.sleep(0.1)
client.loop()

# Publish the birth certificates
publish_birth()

while True:
    # Periodically publish some new data
    payload = get_ddata_payload()

    # Add some random data to the inputs
    add_metric(
        payload,
        None,
        None,
        MetricDataType.String,
        "".join(random.choice(string.ascii_lowercase) for i in range(12)),
    )

    # Note this data we're setting to STALE via the propertyset as an example
    metric = add_metric(
        payload, None, 102, MetricDataType.Boolean, random.choice([True, False])
    )
    metric.properties.keys.extend(["Quality"])
    property_value = metric.properties.values.add()
    property_value.type = ParameterDataType.Int32
    property_value.int_value = 500

    # Publish a message data
    byte_array = bytearray(payload.SerializeToString())

    # Sit and wait for inbound or outbound events
    for _ in range(5):
        time.sleep(0.1)
        client.loop()
