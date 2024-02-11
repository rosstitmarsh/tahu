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

import time
from typing import List

from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.client import MQTTMessage

from .array_packer import (
    convert_to_packed_boolean_array,
    convert_to_packed_datetime_array,
    convert_to_packed_double_array,
    convert_to_packed_float_array,
    convert_to_packed_int8_array,
    convert_to_packed_int16_array,
    convert_to_packed_int32_array,
    convert_to_packed_int64_array,
    convert_to_packed_string_array,
    convert_to_packed_uint8_array,
    convert_to_packed_uint16_array,
    convert_to_packed_uint32_array,
    convert_to_packed_uint64_array,
)
from .sparkplug_b_pb2 import DataType, Payload


class SparkplugDevice:
    def __init__(self, id_: str) -> None:
        self.id = id_

    def get_device_birth_payload(self, seq: int):
        """Get a DBIRTH payload."""
        payload = Payload()
        payload.timestamp = int(round(time.time() * 1000))
        payload.seq = seq
        return payload

    def get_ddata_payload(self, seq: int):
        """Get a DDATA payload."""
        return self.get_device_birth_payload(seq)

    def handle_dcmd(self, client: MQTTClient, payload: Payload, topic_base: str):
        ...

    def publish_device_birth(self, client: MQTTClient, seq: int, topic_base: str):
        raise NotImplementedError

    def publish_ddata(self, client: MQTTClient, seq: int, topic_base: str):
        raise NotImplementedError


class SparkplugNode:
    def __init__(self, id_: str, group_id: str, devices: List[SparkplugDevice]) -> None:
        self.id = id_
        self.group_id = group_id
        self._devices = devices

        self.seq_num = 0
        self.bd_seq = 0

    def on_connect(self, client: MQTTClient, userdata, flags, rc):
        raise NotImplementedError

    def on_message(self, client: MQTTClient, userdata, msg: MQTTMessage):
        raise NotImplementedError

    def get_node_death_payload(self):
        """Get an NDEATH payload.

        Always request this before requesting the Node Birth Payload
        """
        payload = Payload()
        add_metric(payload, "bdSeq", None, DataType.Int64, self.get_bd_seq_num())
        return payload

    def get_node_birth_payload(self):
        """Get an NBIRTH payload.

        Always request this after requesting the Node Death Payload
        """
        self.seq_num = 0
        payload = Payload()
        payload.timestamp = int(round(time.time() * 1000))
        payload.seq = self.get_seq_num()
        add_metric(payload, "bdSeq", None, DataType.Int64, self.bd_seq - 1)
        return payload

    def get_seq_num(self):
        """Get the next sequence number."""
        ret_val = self.seq_num
        self.seq_num += 1
        if self.seq_num == 256:
            self.seq_num = 0
        return ret_val

    def get_bd_seq_num(self):
        """Get the next birth/death sequence number."""
        ret_val = self.bd_seq
        self.bd_seq += 1
        if self.bd_seq == 256:
            self.bd_seq = 0
        return ret_val

    def handle_ncmd(self, client: MQTTClient, payload: Payload):
        ...

    def publish_birth(self, client: MQTTClient):
        raise NotImplementedError

    def publish_node_birth(self, client: MQTTClient):
        raise NotImplementedError


def init_dataset_metric(payload, name, alias, columns, types):
    """Add dataset metrics to a payload."""
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = DataType.DataSet

    # Set up the dataset
    metric.dataset_value.num_of_columns = len(types)
    metric.dataset_value.columns.extend(columns)
    metric.dataset_value.types.extend(types)
    return metric.dataset_value


def init_template_metric(payload, name, alias, template_ref):
    """Add dataset metrics to a payload."""
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = DataType.Template

    # Set up the template
    if template_ref is not None:
        metric.template_value.template_ref = template_ref
        metric.template_value.is_definition = False
    else:
        metric.template_value.is_definition = True

    return metric.template_value


def add_metric(container, name, alias, type_, value, timestamp=None):
    """Add metrics to a container which can be a payload or a template."""
    if timestamp is None:
        timestamp = int(round(time.time() * 1000))

    metric = container.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = timestamp

    if type_ == DataType.Int8:
        metric.datatype = DataType.Int8
        if value < 0:
            value = value + 2**8
        metric.int_value = value
    elif type_ == DataType.Int16:
        metric.datatype = DataType.Int16
        if value < 0:
            value = value + 2**16
        metric.int_value = value
    elif type_ == DataType.Int32:
        metric.datatype = DataType.Int32
        if value < 0:
            value = value + 2**32
        metric.int_value = value
    elif type_ == DataType.Int64:
        metric.datatype = DataType.Int64
        if value < 0:
            value = value + 2**64
        metric.long_value = value
    elif type_ == DataType.UInt8:
        metric.datatype = DataType.UInt8
        metric.int_value = value
    elif type_ == DataType.UInt16:
        metric.datatype = DataType.UInt16
        metric.int_value = value
    elif type_ == DataType.UInt32:
        metric.datatype = DataType.UInt32
        metric.int_value = value
    elif type_ == DataType.UInt64:
        metric.datatype = DataType.UInt64
        metric.long_value = value
    elif type_ == DataType.Float:
        metric.datatype = DataType.Float
        metric.float_value = value
    elif type_ == DataType.Double:
        metric.datatype = DataType.Double
        metric.double_value = value
    elif type_ == DataType.Boolean:
        metric.datatype = DataType.Boolean
        metric.boolean_value = value
    elif type_ == DataType.String:
        metric.datatype = DataType.String
        metric.string_value = value
    elif type_ == DataType.DateTime:
        metric.datatype = DataType.DateTime
        metric.long_value = value
    elif type_ == DataType.Text:
        metric.datatype = DataType.Text
        metric.string_value = value
    elif type_ == DataType.UUID:
        metric.datatype = DataType.UUID
        metric.string_value = value
    elif type_ == DataType.Bytes:
        metric.datatype = DataType.Bytes
        metric.bytes_value = value
    elif type_ == DataType.File:
        metric.datatype = DataType.File
        metric.bytes_value = value
    elif type_ == DataType.Template:
        metric.datatype = DataType.Template
        metric.template_value = value
    elif type_ == DataType.Int8Array:
        metric.datatype = DataType.Int8Array
        metric.bytes_value = convert_to_packed_int8_array(value)
    elif type_ == DataType.Int16Array:
        metric.datatype = DataType.Int16Array
        metric.bytes_value = convert_to_packed_int16_array(value)
    elif type_ == DataType.Int32Array:
        metric.datatype = DataType.Int32Array
        metric.bytes_value = convert_to_packed_int32_array(value)
    elif type_ == DataType.Int64Array:
        metric.datatype = DataType.Int64Array
        metric.bytes_value = convert_to_packed_int64_array(value)
    elif type_ == DataType.UInt8Array:
        metric.datatype = DataType.UInt8Array
        metric.bytes_value = convert_to_packed_uint8_array(value)
    elif type_ == DataType.UInt16Array:
        metric.datatype = DataType.UInt16Array
        metric.bytes_value = convert_to_packed_uint16_array(value)
    elif type_ == DataType.UInt32Array:
        metric.datatype = DataType.UInt32Array
        metric.bytes_value = convert_to_packed_uint32_array(value)
    elif type_ == DataType.UInt64Array:
        metric.datatype = DataType.UInt64Array
        metric.bytes_value = convert_to_packed_uint64_array(value)
    elif type_ == DataType.FloatArray:
        metric.datatype = DataType.FloatArray
        metric.bytes_value = convert_to_packed_float_array(value)
    elif type_ == DataType.DoubleArray:
        metric.datatype = DataType.DoubleArray
        metric.bytes_value = convert_to_packed_double_array(value)
    elif type_ == DataType.BooleanArray:
        metric.datatype = DataType.BooleanArray
        metric.bytes_value = convert_to_packed_boolean_array(value)
    elif type_ == DataType.StringArray:
        metric.datatype = DataType.StringArray
        metric.bytes_value = convert_to_packed_string_array(value)
    elif type_ == DataType.DateTimeArray:
        metric.datatype = DataType.DateTimeArray
        metric.bytes_value = convert_to_packed_datetime_array(value)
    else:
        print(f"Invalid: {type_}")

    # Return the metric
    return metric


def add_historical_metric(container, name, alias, type_, value):
    """Add metrics to a container which can be a payload or a template."""
    metric = add_metric(container, name, alias, type_, value)
    metric.is_historical = True

    # Return the metric
    return metric


def add_null_metric(container, name, alias, type_):
    """Add metrics to a container which can be a payload or a template."""
    metric = container.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.is_null = True

    if type_ == DataType.Int8:
        metric.datatype = DataType.Int8
    elif type_ == DataType.Int16:
        metric.datatype = DataType.Int16
    elif type_ == DataType.Int32:
        metric.datatype = DataType.Int32
    elif type_ == DataType.Int64:
        metric.datatype = DataType.Int64
    elif type_ == DataType.UInt8:
        metric.datatype = DataType.UInt8
    elif type_ == DataType.UInt16:
        metric.datatype = DataType.UInt16
    elif type_ == DataType.UInt32:
        metric.datatype = DataType.UInt32
    elif type_ == DataType.UInt64:
        metric.datatype = DataType.UInt64
    elif type_ == DataType.Float:
        metric.datatype = DataType.Float
    elif type_ == DataType.Double:
        metric.datatype = DataType.Double
    elif type_ == DataType.Boolean:
        metric.datatype = DataType.Boolean
    elif type_ == DataType.String:
        metric.datatype = DataType.String
    elif type_ == DataType.DateTime:
        metric.datatype = DataType.DateTime
    elif type_ == DataType.Text:
        metric.datatype = DataType.Text
    elif type_ == DataType.UUID:
        metric.datatype = DataType.UUID
    elif type_ == DataType.Bytes:
        metric.datatype = DataType.Bytes
    elif type_ == DataType.File:
        metric.datatype = DataType.File
    elif type_ == DataType.Template:
        metric.datatype = DataType.Template
    elif type_ == DataType.Int8Array:
        metric.datatype = DataType.Int8Array
    elif type_ == DataType.Int16Array:
        metric.datatype = DataType.Int16Array
    elif type_ == DataType.Int32Array:
        metric.datatype = DataType.Int32Array
    elif type_ == DataType.Int64Array:
        metric.datatype = DataType.Int64Array
    elif type_ == DataType.UInt8Array:
        metric.datatype = DataType.UInt8Array
    elif type_ == DataType.UInt16Array:
        metric.datatype = DataType.UInt16Array
    elif type_ == DataType.UInt32Array:
        metric.datatype = DataType.UInt32Array
    elif type_ == DataType.UInt64Array:
        metric.datatype = DataType.UInt64Array
    elif type_ == DataType.FloatArray:
        metric.datatype = DataType.FloatArray
    elif type_ == DataType.DoubleArray:
        metric.datatype = DataType.DoubleArray
    elif type_ == DataType.BooleanArray:
        metric.datatype = DataType.BooleanArray
    elif type_ == DataType.StringArray:
        metric.datatype = DataType.StringArray
    elif type_ == DataType.DateTimeArray:
        metric.datatype = DataType.DateTimeArray
    else:
        print(f"Invalid: {type_}")

    # Return the metric
    return metric
