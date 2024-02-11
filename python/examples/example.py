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
    DataSetDataType,
    MetricDataType,
    ParameterDataType,
    SparkplugNode,
    add_metric,
    add_null_metric,
    init_dataset_metric,
    init_template_metric,
)
from core.sparkplug_b_pb2 import Payload


class AliasMap:
    Next_Server = 0
    Rebirth = 1
    Reboot = 2
    Dataset = 3
    Node_Metric0 = 4
    Node_Metric1 = 5
    Node_Metric2 = 6
    Node_Metric3 = 7
    Device_Metric0 = 8
    Device_Metric1 = 9
    Device_Metric2 = 10
    Device_Metric3 = 11
    My_Custom_Motor = 12


class ExampleNode(SparkplugNode):
    def __init__(self, group_id, node_name, device_name) -> None:
        super().__init__()
        self.group_id = group_id
        self.node_name = node_name
        self.device_name = device_name

    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server."""
        if rc == 0:
            print("Connected with result code " + str(rc))
        else:
            print("Failed to connect with result code " + str(rc))
            sys.exit()

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(f"spBv1.0/{self.group_id}/NCMD/{self.node_name}/#")
        client.subscribe(f"spBv1.0/{self.group_id}/DCMD/{self.node_name}/#")

    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server."""
        print("Message arrived: " + msg.topic)
        tokens = msg.topic.split("/")

        if (
            tokens[0] == "spBv1.0"
            and tokens[1] == self.group_id
            and (tokens[2] == "NCMD" or tokens[2] == "DCMD")
            and tokens[3] == self.node_name
        ):
            inbound_payload = Payload()
            inbound_payload.ParseFromString(msg.payload)
            for metric in inbound_payload.metrics:
                if (
                    metric.name == "Node Control/Next Server"
                    or metric.alias == AliasMap.Next_Server
                ):
                    # 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                    # disconnect from the current MQTT server and connect to the next MQTT server in the
                    # list of available servers.  This is used for clients that have a pool of MQTT servers
                    # to connect to.
                    print(
                        "'Node Control/Next Server' is not implemented in this example"
                    )
                elif (
                    metric.name == "Node Control/Rebirth"
                    or metric.alias == AliasMap.Rebirth
                ):
                    # 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                    # its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                    # application if it receives an NDATA or DDATA with a metric that was not published in the
                    # original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                    # its original NBIRTH and DBIRTH messages.
                    self.publish_birth()
                elif (
                    metric.name == "Node Control/Reboot"
                    or metric.alias == AliasMap.Reboot
                ):
                    # 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                    # This can be used for devices that need a full application reset via a soft reboot.
                    # In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                    # messages.
                    self.publish_birth()
                elif (
                    metric.name == "output/Device Metric2"
                    or metric.alias == AliasMap.Device_Metric2
                ):
                    # This is a metric we declared in our DBIRTH message and we're emulating an output.
                    # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                    # value.  If this were a real output we'd write to the output and then read it back
                    # before publishing a DDATA message.

                    # We know this is an Int16 because of how we declated it in the DBIRTH
                    new_value = metric.int_value
                    print(
                        f"CMD message for output/Device Metric2 - New Value: {new_value}"
                    )

                    # Create the DDATA payload - Use the alias because this isn't the DBIRTH
                    payload = self.get_ddata_payload()
                    add_metric(
                        payload,
                        None,
                        AliasMap.Device_Metric2,
                        MetricDataType.Int16,
                        new_value,
                    )

                    # Publish a message data
                    byte_array = bytearray(payload.SerializeToString())
                    client.publish(
                        f"spBv1.0/{self.group_id}/DDATA/{self.node_name}/{self.device_name}",
                        byte_array,
                        0,
                        False,
                    )
                elif (
                    metric.name == "output/Device Metric3"
                    or metric.alias == AliasMap.Device_Metric3
                ):
                    # This is a metric we declared in our DBIRTH message and we're emulating an output.
                    # So, on incoming 'writes' to the output we must publish a DDATA with the new output
                    # value.  If this were a real output we'd write to the output and then read it back
                    # before publishing a DDATA message.

                    # We know this is an Boolean because of how we declated it in the DBIRTH
                    new_value = metric.boolean_value
                    print(
                        f"CMD message for output/Device Metric3 - New Value: {new_value}"
                    )

                    # Create the DDATA payload - use the alias because this isn't the DBIRTH
                    payload = self.get_ddata_payload()
                    add_metric(
                        payload,
                        None,
                        AliasMap.Device_Metric3,
                        MetricDataType.Boolean,
                        new_value,
                    )

                    # Publish a message data
                    byte_array = bytearray(payload.SerializeToString())
                    client.publish(
                        f"spBv1.0/{self.group_id}/DDATA/{self.node_name}/{self.device_name}",
                        byte_array,
                        0,
                        False,
                    )
                else:
                    print("Unknown command: " + metric.name)
        else:
            print("Unknown command...")

        print("Done publishing")

    def publish_birth(self, client):
        """Publish the BIRTH certificates."""
        self.publish_node_birth(client)
        self.publish_device_birth(client)

    def publish_node_birth(self, client):
        """Publish the NBIRTH certificate."""
        print("Publishing Node Birth")

        # Create the node birth payload
        payload = self.get_node_birth_payload()

        # Set up the Node Controls
        add_metric(
            payload,
            "Node Control/Next Server",
            AliasMap.Next_Server,
            MetricDataType.Boolean,
            False,
        )
        add_metric(
            payload,
            "Node Control/Rebirth",
            AliasMap.Rebirth,
            MetricDataType.Boolean,
            False,
        )
        add_metric(
            payload,
            "Node Control/Reboot",
            AliasMap.Reboot,
            MetricDataType.Boolean,
            False,
        )

        # Add some regular node metrics
        add_metric(
            payload,
            "Node Metric0",
            AliasMap.Node_Metric0,
            MetricDataType.String,
            "hello node",
        )
        add_metric(
            payload, "Node Metric1", AliasMap.Node_Metric1, MetricDataType.Boolean, True
        )
        add_null_metric(
            payload, "Node Metric3", AliasMap.Node_Metric3, MetricDataType.Int32
        )

        # Create a DataSet (012 - 345) two rows with Int8, Int16, and Int32 contents and headers Int8s, Int16s, Int32s and add it to the payload
        columns = ["Int8s", "Int16s", "Int32s"]
        types = [DataSetDataType.Int8, DataSetDataType.Int16, DataSetDataType.Int32]
        dataset = init_dataset_metric(
            payload, "DataSet", AliasMap.Dataset, columns, types
        )
        row = dataset.rows.add()
        element = row.elements.add()
        element.int_value = 0
        element = row.elements.add()
        element.int_value = 1
        element = row.elements.add()
        element.int_value = 2
        row = dataset.rows.add()
        element = row.elements.add()
        element.int_value = 3
        element = row.elements.add()
        element.int_value = 4
        element = row.elements.add()
        element.int_value = 5

        # Add a metric with a custom property
        metric = add_metric(
            payload, "Node Metric2", AliasMap.Node_Metric2, MetricDataType.Int16, 13
        )
        metric.properties.keys.extend(["engUnit"])
        property_value = metric.properties.values.add()
        property_value.type = ParameterDataType.String
        property_value.string_value = "MyCustomUnits"

        # Create the UDT definition value which includes two UDT members and a single parameter and add it to the payload
        template = init_template_metric(
            payload, "_types_/Custom_Motor", None, None
        )  # No alias for Template definitions
        template_parameter = template.parameters.add()
        template_parameter.name = "Index"
        template_parameter.type = ParameterDataType.String
        template_parameter.string_value = "0"
        add_metric(
            template, "RPMs", None, MetricDataType.Int32, 0
        )  # No alias in UDT members
        add_metric(
            template, "AMPs", None, MetricDataType.Int32, 0
        )  # No alias in UDT members

        # Publish the node birth certificate
        byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"spBv1.0/{self.group_id}/NBIRTH/{self.node_name}", byte_array, 0, False
        )

    def publish_device_birth(self, client):
        """Publish the DBIRTH certificate."""
        print("Publishing Device Birth")

        # Get the payload
        payload = self.get_device_birth_payload()

        # Add some device metrics
        add_metric(
            payload,
            "input/Device Metric0",
            AliasMap.Device_Metric0,
            MetricDataType.String,
            "hello device",
        )
        add_metric(
            payload,
            "input/Device Metric1",
            AliasMap.Device_Metric1,
            MetricDataType.Boolean,
            True,
        )
        add_metric(
            payload,
            "output/Device Metric2",
            AliasMap.Device_Metric2,
            MetricDataType.Int16,
            16,
        )
        add_metric(
            payload,
            "output/Device Metric3",
            AliasMap.Device_Metric3,
            MetricDataType.Boolean,
            True,
        )

        # Create the UDT definition value which includes two UDT members and a single parameter and add it to the payload
        template = init_template_metric(
            payload, "My_Custom_Motor", AliasMap.My_Custom_Motor, "Custom_Motor"
        )
        template_parameter = template.parameters.add()
        template_parameter.name = "Index"
        template_parameter.type = ParameterDataType.String
        template_parameter.string_value = "1"
        add_metric(
            template, "RPMs", None, MetricDataType.Int32, 123
        )  # No alias in UDT members
        add_metric(
            template, "AMPs", None, MetricDataType.Int32, 456
        )  # No alias in UDT members

        # Publish the initial data with the Device BIRTH certificate
        total_byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"spBv1.0/{self.group_id}/DBIRTH/{self.node_name}/{self.device_name}",
            total_byte_array,
            0,
            False,
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

    node = ExampleNode(group_id, node_name, device_name)

    # Start of main program - Set up the MQTT client connection
    client = mqtt.Client(server_url, 1883, 60)

    client.on_connect = node.on_connect
    client.on_message = node.on_message

    # Create the node death payload
    death_payload = node.get_node_death_payload()
    death_byte_array = bytearray(death_payload.SerializeToString())
    death_topic = f"spBv1.0/{node.group_id}/NDEATH/{node.node_name}"
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
        payload = node.get_ddata_payload()

        # Add some random data to the inputs
        add_metric(
            payload,
            None,
            AliasMap.Device_Metric0,
            MetricDataType.String,
            "".join(random.choice(string.ascii_lowercase) for _ in range(12)),
        )

        # Note this data we're setting to STALE via the propertyset as an example
        metric = add_metric(
            payload,
            None,
            AliasMap.Device_Metric1,
            MetricDataType.Boolean,
            random.choice([True, False]),
        )
        metric.properties.keys.extend(["Quality"])
        property_value = metric.properties.values.add()
        property_value.type = ParameterDataType.Int32
        property_value.int_value = 500

        # Publish a message data
        byte_array = bytearray(payload.SerializeToString())
        client.publish(
            f"spBv1.0/{node.group_id}/DDATA/{node.node_name}/{node.device_name}",
            byte_array,
            0,
            False,
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
