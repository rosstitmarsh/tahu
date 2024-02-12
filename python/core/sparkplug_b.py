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
from typing import Any, List, Optional

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

    def get_device_birth_payload(self, seq: int) -> Payload:
        """Get a DBIRTH payload."""
        return Payload(
            timestamp=int(round(time.time() * 1000)),
            seq=seq,
        )

    def get_ddata_payload(self, seq: int) -> Payload:
        """Get a DDATA payload."""
        return self.get_device_birth_payload(seq)

    def handle_dcmd(
        self, client: MQTTClient, payload: Payload, topic_base: str
    ) -> None:
        ...

    def publish_device_birth(
        self, client: MQTTClient, seq: int, topic_base: str
    ) -> None:
        raise NotImplementedError

    def publish_ddata(self, client: MQTTClient, seq: int, topic_base: str) -> None:
        raise NotImplementedError


class SparkplugNode:
    def __init__(self, id_: str, group_id: str, devices: List[SparkplugDevice]) -> None:
        self.id = id_
        self.group_id = group_id
        self._devices = devices

        self.seq_num = 0
        self.bd_seq = 0

    def on_connect(self, client: MQTTClient, userdata, flags, rc) -> None:
        raise NotImplementedError

    def on_message(self, client: MQTTClient, userdata, msg: MQTTMessage) -> None:
        raise NotImplementedError

    def get_node_death_payload(self) -> Payload:
        """Get an NDEATH payload.

        Always request this before requesting the Node Birth Payload
        """
        return Payload(
            metrics=[
                create_metric("bdSeq", None, DataType.Int64, self.get_bd_seq_num())
            ]
        )

    def get_node_birth_payload(self) -> Payload:
        """Get an NBIRTH payload.

        Always request this after requesting the Node Death Payload
        """
        self.seq_num = 0
        return Payload(
            timestamp=int(round(time.time() * 1000)),
            metrics=[create_metric("bdSeq", None, DataType.Int64, self.bd_seq - 1)],
            seq=self.get_seq_num(),
        )

    def get_seq_num(self) -> int:
        """Get the next sequence number."""
        ret_val = self.seq_num
        self.seq_num += 1
        if self.seq_num == 256:
            self.seq_num = 0
        return ret_val

    def get_bd_seq_num(self) -> int:
        """Get the next birth/death sequence number."""
        ret_val = self.bd_seq
        self.bd_seq += 1
        if self.bd_seq == 256:
            self.bd_seq = 0
        return ret_val

    def handle_ncmd(self, client: MQTTClient, payload: Payload) -> None:
        ...

    def publish_birth(self, client: MQTTClient) -> None:
        raise NotImplementedError

    def publish_node_birth(self, client: MQTTClient) -> None:
        raise NotImplementedError


def create_metric(
    name: Optional[str],
    alias: Optional[int],
    type_: DataType,
    value: Optional[Any],
    timestamp: Optional[int] = None,
    is_historical: bool = False,
    is_transient: bool = False,
    metadata: Optional[Payload.MetaData] = None,
    properties: Optional[Payload.PropertySet] = None,
) -> Payload.Metric:
    """Create a metric."""
    if timestamp is None:
        timestamp = int(round(time.time() * 1000))

    metric = Payload.Metric(
        name=name,
        alias=alias,
        timestamp=timestamp,
        datatype=type_,
        is_historical=is_historical,
        is_transient=is_transient,
        is_null=value is None,
        metadata=metadata,
        properties=properties,
    )

    if metric.is_null:
        return metric

    if type_ == DataType.Int8:
        if value < 0:
            value = value + 2**8
        metric.int_value = value
    elif type_ == DataType.Int16:
        if value < 0:
            value = value + 2**16
        metric.int_value = value
    elif type_ == DataType.Int32:
        if value < 0:
            value = value + 2**32
        metric.int_value = value
    elif type_ == DataType.Int64:
        if value < 0:
            value = value + 2**64
        metric.long_value = value
    elif type_ in [DataType.UInt8, DataType.UInt16, DataType.UInt32]:
        metric.int_value = value
    elif type_ == DataType.UInt64:
        metric.long_value = value
    elif type_ == DataType.Float:
        metric.float_value = value
    elif type_ == DataType.Double:
        metric.double_value = value
    elif type_ == DataType.Boolean:
        metric.boolean_value = value
    elif type_ == DataType.String:
        metric.string_value = value
    elif type_ == DataType.DateTime:
        metric.long_value = value
    elif type_ in [DataType.Text, DataType.UUID]:
        metric.string_value = value
    elif type_ in [DataType.Bytes, DataType.File]:
        metric.bytes_value = value
    elif type_ == DataType.Template:
        metric.template_value.CopyFrom(value)
    elif type_ == DataType.DataSet:
        metric.dataset_value.CopyFrom(value)
    elif type_ == DataType.Int8Array:
        metric.bytes_value = convert_to_packed_int8_array(value)
    elif type_ == DataType.Int16Array:
        metric.bytes_value = convert_to_packed_int16_array(value)
    elif type_ == DataType.Int32Array:
        metric.bytes_value = convert_to_packed_int32_array(value)
    elif type_ == DataType.Int64Array:
        metric.bytes_value = convert_to_packed_int64_array(value)
    elif type_ == DataType.UInt8Array:
        metric.bytes_value = convert_to_packed_uint8_array(value)
    elif type_ == DataType.UInt16Array:
        metric.bytes_value = convert_to_packed_uint16_array(value)
    elif type_ == DataType.UInt32Array:
        metric.bytes_value = convert_to_packed_uint32_array(value)
    elif type_ == DataType.UInt64Array:
        metric.bytes_value = convert_to_packed_uint64_array(value)
    elif type_ == DataType.FloatArray:
        metric.bytes_value = convert_to_packed_float_array(value)
    elif type_ == DataType.DoubleArray:
        metric.bytes_value = convert_to_packed_double_array(value)
    elif type_ == DataType.BooleanArray:
        metric.bytes_value = convert_to_packed_boolean_array(value)
    elif type_ == DataType.StringArray:
        metric.bytes_value = convert_to_packed_string_array(value)
    elif type_ == DataType.DateTimeArray:
        metric.bytes_value = convert_to_packed_datetime_array(value)
    else:
        print(f"Invalid: {type_}")

    return metric
