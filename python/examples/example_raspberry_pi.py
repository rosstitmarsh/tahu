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

import subprocess
import time
from threading import Lock

import paho.mqtt.client as mqtt
import pibrella

from core.sparkplug_b import (
    MetricDataType,
    add_metric,
    get_ddata_payload,
    get_device_birth_payload,
    get_node_birth_payload,
    get_node_death_payload,
)
from core.sparkplug_b_pb2 import Payload

server_url = "192.168.1.53"
my_group_id = "Sparkplug B Devices"
my_node_name = "Python Raspberry Pi"
my_sub_node_name = "Pibrella"
my_username = "admin"
my_passowrd = "changeme"
lock = Lock()


# Button press event handler
def button_changed(pin):
    outbound_payload = get_ddata_payload()
    button_value = pin.read()
    if button_value == 1:
        print("You pressed the button!")
    else:
        print("You released the button!")
    add_metric(outbound_payload, "button", None, MetricDataType.Boolean, button_value)
    byte_array = bytearray(outbound_payload.SerializeToString())
    client.publish(
        f"spBv1.0/{my_group_id}/DDATA/{my_node_name}/{my_sub_node_name}",
        byte_array,
        0,
        False,
    )


# Input change event handler
def input_a_changed(pin):
    input_changed("Inputs/a", pin)


def input_b_changed(pin):
    input_changed("Inputs/b", pin)


def input_c_changed(pin):
    input_changed("Inputs/c", pin)


def input_d_changed(pin):
    input_changed("Inputs/d", pin)


def input_changed(name, pin):
    lock.acquire()
    try:
        # Lock the block around the callback handler to prevent inproper access based on debounce
        outbound_payload = get_ddata_payload()
        add_metric(outbound_payload, name, None, MetricDataType.Boolean, pin.read())
        byte_array = bytearray(outbound_payload.SerializeToString())
        client.publish(
            f"spBv1.0/{my_group_id}/DDATA/{my_node_name}/{my_sub_node_name}",
            byte_array,
            0,
            False,
        )
    finally:
        lock.release()


def on_connect(client, userdata, flags, rc):
    """Callback for when the client receives a CONNACK response from the server."""
    global my_group_id
    global my_node_name
    print("Connected with result code " + str(rc))

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
        and tokens[2] == "DCMD"
        and tokens[3] == my_node_name
    ):
        inbound_payload = Payload()
        inbound_payload.ParseFromString(msg.payload)
        outbound_payload = get_ddata_payload()

        for metric in inbound_payload.metrics:
            print("Tag Name: " + metric.name)
            if metric.name == "Outputs/e":
                pibrella.output.e.write(metric.boolean_value)
                add_metric(
                    outbound_payload,
                    "Outputs/e",
                    None,
                    MetricDataType.Boolean,
                    pibrella.output.e.read(),
                )
            elif metric.name == "Outputs/f":
                pibrella.output.f.write(metric.boolean_value)
                add_metric(
                    outbound_payload,
                    "Outputs/f",
                    None,
                    MetricDataType.Boolean,
                    pibrella.output.f.read(),
                )
            elif metric.name == "Outputs/g":
                pibrella.output.g.write(metric.boolean_value)
                add_metric(
                    outbound_payload,
                    "Outputs/g",
                    None,
                    MetricDataType.Boolean,
                    pibrella.output.g.read(),
                )
            elif metric.name == "Outputs/h":
                pibrella.output.h.write(metric.boolean_value)
                add_metric(
                    outbound_payload,
                    "Outputs/h",
                    None,
                    MetricDataType.Boolean,
                    pibrella.output.h.read(),
                )
            elif metric.name == "Outputs/LEDs/green":
                if metric.boolean_value:
                    pibrella.light.green.on()
                else:
                    pibrella.light.green.off()
                add_metric(
                    outbound_payload,
                    "Outputs/LEDs/green",
                    None,
                    MetricDataType.Boolean,
                    pibrella.light.green.read(),
                )
            elif metric.name == "Outputs/LEDs/red":
                if metric.boolean_value:
                    pibrella.light.red.on()
                else:
                    pibrella.light.red.off()
                add_metric(
                    outbound_payload,
                    "Outputs/LEDs/red",
                    None,
                    MetricDataType.Boolean,
                    pibrella.light.red.read(),
                )
            elif metric.name == "Outputs/LEDs/yellow":
                if metric.boolean_value:
                    pibrella.light.yellow.on()
                else:
                    pibrella.light.yellow.off()
                add_metric(
                    outbound_payload,
                    "Outputs/LEDs/yellow",
                    None,
                    MetricDataType.Boolean,
                    pibrella.light.yellow.read(),
                )
            elif metric.name == "buzzer_fail":
                pibrella.buzzer.fail()
            elif metric.name == "buzzer_success":
                pibrella.buzzer.success()

        byte_array = bytearray(outbound_payload.SerializeToString())
        client.publish(
            f"spBv1.0/{my_group_id}/DDATA/{my_node_name}/{my_sub_node_name}",
            byte_array,
            0,
            False,
        )
    elif (
        tokens[0] == "spBv1.0"
        and tokens[1] == my_group_id
        and tokens[2] == "NCMD"
        and tokens[3] == my_node_name
    ):
        inbound_payload = Payload()
        inbound_payload.ParseFromString(msg.payload)
        for metric in inbound_payload.metrics:
            if metric.name == "Node Control/Next Server":
                publish_births()
            if metric.name == "Node Control/Rebirth":
                publish_births()
            if metric.name == "Node Control/Reboot":
                publish_births()
    else:
        print("Unknown command...")

    print("done publishing")


#
def publish_births():
    """Publish the Birth certificate."""
    print("Publishing Birth")

    # Create the NBIRTH payload
    payload = get_node_birth_payload()

    # Add the Node Controls
    add_metric(payload, "Node Control/Next Server", None, MetricDataType.Boolean, False)
    add_metric(payload, "Node Control/Rebirth", None, MetricDataType.Boolean, False)
    add_metric(payload, "Node Control/Reboot", None, MetricDataType.Boolean, False)

    # Set up the device Parameters
    p = subprocess.Popen(
        "uname -a", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    for line in p.stdout.readlines():
        uname_output = (line,)
    ret_val = p.wait()
    p = subprocess.Popen(
        "cat /proc/cpuinfo | grep Hardware",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in p.stdout.readlines():
        hardware_output = (line,)
    ret_val = p.wait()
    p = subprocess.Popen(
        "cat /proc/cpuinfo | grep Revision",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in p.stdout.readlines():
        revision_output = (line,)
    ret_val = p.wait()
    p = subprocess.Popen(
        "cat /proc/cpuinfo | grep Serial",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in p.stdout.readlines():
        serial_output = (line,)
    ret_val = p.wait()
    add_metric(
        payload,
        "Parameters/sw_version",
        None,
        MetricDataType.String,
        "".join(uname_output),
    )
    add_metric(
        payload,
        "Parameters/hw_version",
        None,
        MetricDataType.String,
        "".join(hardware_output),
    )
    add_metric(
        payload,
        "Parameters/hw_revision",
        None,
        MetricDataType.String,
        "".join(revision_output),
    )
    add_metric(
        payload,
        "Parameters/hw_serial",
        None,
        MetricDataType.String,
        "".join(serial_output),
    )

    # Publish the NBIRTH certificate
    byte_array = bytearray(payload.SerializeToString())
    client.publish(f"spBv1.0/{my_group_id}/NBIRTH/{my_node_name}", byte_array, 0, False)

    # Set up the DBIRTH with the input metrics
    payload = get_device_birth_payload()

    add_metric(
        payload, "Inputs/a", None, MetricDataType.Boolean, pibrella.input.a.read()
    )
    add_metric(
        payload, "Inputs/b", None, MetricDataType.Boolean, pibrella.input.b.read()
    )
    add_metric(
        payload, "Inputs/c", None, MetricDataType.Boolean, pibrella.input.c.read()
    )
    add_metric(
        payload, "Inputs/d", None, MetricDataType.Boolean, pibrella.input.d.read()
    )

    # Set up the output states on first run so Ignition and MQTT Engine are aware of them
    add_metric(
        payload, "Outputs/e", None, MetricDataType.Boolean, pibrella.output.e.read()
    )
    add_metric(
        payload, "Outputs/f", None, MetricDataType.Boolean, pibrella.output.f.read()
    )
    add_metric(
        payload, "Outputs/g", None, MetricDataType.Boolean, pibrella.output.g.read()
    )
    add_metric(
        payload, "Outputs/h", None, MetricDataType.Boolean, pibrella.output.h.read()
    )
    add_metric(
        payload,
        "Outputs/LEDs/green",
        None,
        MetricDataType.Boolean,
        pibrella.light.green.read(),
    )
    add_metric(
        payload,
        "Outputs/LEDs/red",
        None,
        MetricDataType.Boolean,
        pibrella.light.red.read(),
    )
    add_metric(
        payload,
        "Outputs/LEDs/yellow",
        None,
        MetricDataType.Boolean,
        pibrella.light.yellow.read(),
    )
    add_metric(payload, "button", None, MetricDataType.Boolean, pibrella.button.read())
    add_metric(payload, "buzzer_fail", None, MetricDataType.Boolean, 0)
    add_metric(payload, "buzzer_success", None, MetricDataType.Boolean, 0)

    # Publish the initial data with the DBIRTH certificate
    total_byte_array = bytearray(payload.SerializeToString())
    client.publish(
        f"spBv1.0/{my_group_id}/DBIRTH/{my_node_name}/{my_sub_node_name}",
        total_byte_array,
        0,
        False,
    )


# Create the NDEATH payload
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

publish_births()

# Set up the button press event handler
pibrella.button.changed(button_changed)
pibrella.input.a.changed(input_a_changed)
pibrella.input.b.changed(input_b_changed)
pibrella.input.c.changed(input_c_changed)
pibrella.input.d.changed(input_d_changed)

# Sit and wait for inbound or outbound events
while True:
    time.sleep(0.1)
    client.loop()
