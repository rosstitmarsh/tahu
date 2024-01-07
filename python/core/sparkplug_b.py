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
from .sparkplug_b_pb2 import Payload

seqNum = 0
bdSeq = 0


class DataSetDataType:
    Unknown = 0
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14


class MetricDataType:
    Unknown = 0
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14
    UUID = 15
    DataSet = 16
    Bytes = 17
    File = 18
    Template = 19


class ParameterDataType:
    Unknown = 0
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14


class ParameterDataType:
    Unknown = 0
    Int8 = 1
    Int16 = 2
    Int32 = 3
    Int64 = 4
    UInt8 = 5
    UInt16 = 6
    UInt32 = 7
    UInt64 = 8
    Float = 9
    Double = 10
    Boolean = 11
    String = 12
    DateTime = 13
    Text = 14


def getNodeDeathPayload():
    """Get an NDEATH payload.

    Always request this before requesting the Node Birth Payload
    """
    payload = Payload()
    addMetric(payload, "bdSeq", None, MetricDataType.Int64, getBdSeqNum())
    return payload


def getNodeBirthPayload():
    """Get an NBIRTH payload.

    Always request this after requesting the Node Death Payload
    """
    global seqNum
    seqNum = 0
    payload = Payload()
    payload.timestamp = int(round(time.time() * 1000))
    payload.seq = getSeqNum()
    addMetric(payload, "bdSeq", None, MetricDataType.Int64, bdSeq - 1)
    return payload


def getDeviceBirthPayload():
    """Get a DBIRTH payload."""
    payload = Payload()
    payload.timestamp = int(round(time.time() * 1000))
    payload.seq = getSeqNum()
    return payload


def getDdataPayload():
    """Get a DDATA payload."""
    return getDeviceBirthPayload()


def initDatasetMetric(payload, name, alias, columns, types):
    """Add dataset metrics to a payload."""
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = MetricDataType.DataSet

    # Set up the dataset
    metric.dataset_value.num_of_columns = len(types)
    metric.dataset_value.columns.extend(columns)
    metric.dataset_value.types.extend(types)
    return metric.dataset_value


def initTemplateMetric(payload, name, alias, templateRef):
    """Add dataset metrics to a payload."""
    metric = payload.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.datatype = MetricDataType.Template

    # Set up the template
    if templateRef is not None:
        metric.template_value.template_ref = templateRef
        metric.template_value.is_definition = False
    else:
        metric.template_value.is_definition = True

    return metric.template_value


# def addMetric(container, name, alias, type, value):
#     """Add metrics to a container which can be a payload or a template"""
#     metric.timestamp = int(round(time.time() * 1000))
#     return addMetric(container, name, alias, type, value, timestamp)


def addMetric(container, name, alias, type_, value, timestamp=None):
    """Add metrics to a container which can be a payload or a template."""
    if timestamp is None:
        timestamp = int(round(time.time() * 1000))

    metric = container.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = timestamp

    # print( "Type: " + str(type))

    if type_ == MetricDataType.Int8:
        metric.datatype = MetricDataType.Int8
        if value < 0:
            value = value + 2**8
        metric.int_value = value
    elif type_ == MetricDataType.Int16:
        metric.datatype = MetricDataType.Int16
        if value < 0:
            value = value + 2**16
        metric.int_value = value
    elif type_ == MetricDataType.Int32:
        metric.datatype = MetricDataType.Int32
        if value < 0:
            value = value + 2**32
        metric.int_value = value
    elif type_ == MetricDataType.Int64:
        metric.datatype = MetricDataType.Int64
        if value < 0:
            value = value + 2**64
        metric.long_value = value
    elif type_ == MetricDataType.UInt8:
        metric.datatype = MetricDataType.UInt8
        metric.int_value = value
    elif type_ == MetricDataType.UInt16:
        metric.datatype = MetricDataType.UInt16
        metric.int_value = value
    elif type_ == MetricDataType.UInt32:
        metric.datatype = MetricDataType.UInt32
        metric.int_value = value
    elif type_ == MetricDataType.UInt64:
        metric.datatype = MetricDataType.UInt64
        metric.long_value = value
    elif type_ == MetricDataType.Float:
        metric.datatype = MetricDataType.Float
        metric.float_value = value
    elif type_ == MetricDataType.Double:
        metric.datatype = MetricDataType.Double
        metric.double_value = value
    elif type_ == MetricDataType.Boolean:
        metric.datatype = MetricDataType.Boolean
        metric.boolean_value = value
    elif type_ == MetricDataType.String:
        metric.datatype = MetricDataType.String
        metric.string_value = value
    elif type_ == MetricDataType.DateTime:
        metric.datatype = MetricDataType.DateTime
        metric.long_value = value
    elif type_ == MetricDataType.Text:
        metric.datatype = MetricDataType.Text
        metric.string_value = value
    elif type_ == MetricDataType.UUID:
        metric.datatype = MetricDataType.UUID
        metric.string_value = value
    elif type_ == MetricDataType.Bytes:
        metric.datatype = MetricDataType.Bytes
        metric.bytes_value = value
    elif type_ == MetricDataType.File:
        metric.datatype = MetricDataType.File
        metric.bytes_value = value
    elif type_ == MetricDataType.Template:
        metric.datatype = MetricDataType.Template
        metric.template_value = value
    elif type_ == MetricDataType.Int8Array:
        metric.datatype = MetricDataType.Int8Array
        metric.bytes_value = convert_to_packed_int8_array(value)
    elif type_ == MetricDataType.Int16Array:
        metric.datatype = MetricDataType.Int16Array
        metric.bytes_value = convert_to_packed_int16_array(value)
    elif type_ == MetricDataType.Int32Array:
        metric.datatype = MetricDataType.Int32Array
        metric.bytes_value = convert_to_packed_int32_array(value)
    elif type_ == MetricDataType.Int64Array:
        metric.datatype = MetricDataType.Int64Array
        metric.bytes_value = convert_to_packed_int64_array(value)
    elif type_ == MetricDataType.UInt8Array:
        metric.datatype = MetricDataType.UInt8Array
        metric.bytes_value = convert_to_packed_uint8_array(value)
    elif type_ == MetricDataType.UInt16Array:
        metric.datatype = MetricDataType.UInt16Array
        metric.bytes_value = convert_to_packed_uint16_array(value)
    elif type_ == MetricDataType.UInt32Array:
        metric.datatype = MetricDataType.UInt32Array
        metric.bytes_value = convert_to_packed_uint32_array(value)
    elif type_ == MetricDataType.UInt64Array:
        metric.datatype = MetricDataType.UInt64Array
        metric.bytes_value = convert_to_packed_uint64_array(value)
    elif type_ == MetricDataType.FloatArray:
        metric.datatype = MetricDataType.FloatArray
        metric.bytes_value = convert_to_packed_float_array(value)
    elif type_ == MetricDataType.DoubleArray:
        metric.datatype = MetricDataType.DoubleArray
        metric.bytes_value = convert_to_packed_double_array(value)
    elif type_ == MetricDataType.BooleanArray:
        metric.datatype = MetricDataType.BooleanArray
        metric.bytes_value = convert_to_packed_boolean_array(value)
    elif type_ == MetricDataType.StringArray:
        metric.datatype = MetricDataType.StringArray
        metric.bytes_value = convert_to_packed_string_array(value)
    elif type_ == MetricDataType.DateTimeArray:
        metric.datatype = MetricDataType.DateTimeArray
        metric.bytes_value = convert_to_packed_datetime_array(value)
    else:
        print("Invalid: " + str(type_))

    # Return the metric
    return metric


def addHistoricalMetric(container, name, alias, type_, value):
    """Add metrics to a container which can be a payload or a template."""
    metric = addMetric(container, name, alias, type_, value)
    metric.is_historical = True

    # Return the metric
    return metric


def addNullMetric(container, name, alias, type_):
    """Add metrics to a container which can be a payload or a template."""
    metric = container.metrics.add()
    if name is not None:
        metric.name = name
    if alias is not None:
        metric.alias = alias
    metric.timestamp = int(round(time.time() * 1000))
    metric.is_null = True

    # print( "Type: " + str(type))

    if type_ == MetricDataType.Int8:
        metric.datatype = MetricDataType.Int8
    elif type_ == MetricDataType.Int16:
        metric.datatype = MetricDataType.Int16
    elif type_ == MetricDataType.Int32:
        metric.datatype = MetricDataType.Int32
    elif type_ == MetricDataType.Int64:
        metric.datatype = MetricDataType.Int64
    elif type_ == MetricDataType.UInt8:
        metric.datatype = MetricDataType.UInt8
    elif type_ == MetricDataType.UInt16:
        metric.datatype = MetricDataType.UInt16
    elif type_ == MetricDataType.UInt32:
        metric.datatype = MetricDataType.UInt32
    elif type_ == MetricDataType.UInt64:
        metric.datatype = MetricDataType.UInt64
    elif type_ == MetricDataType.Float:
        metric.datatype = MetricDataType.Float
    elif type_ == MetricDataType.Double:
        metric.datatype = MetricDataType.Double
    elif type_ == MetricDataType.Boolean:
        metric.datatype = MetricDataType.Boolean
    elif type_ == MetricDataType.String:
        metric.datatype = MetricDataType.String
    elif type_ == MetricDataType.DateTime:
        metric.datatype = MetricDataType.DateTime
    elif type_ == MetricDataType.Text:
        metric.datatype = MetricDataType.Text
    elif type_ == MetricDataType.UUID:
        metric.datatype = MetricDataType.UUID
    elif type_ == MetricDataType.Bytes:
        metric.datatype = MetricDataType.Bytes
    elif type_ == MetricDataType.File:
        metric.datatype = MetricDataType.File
    elif type_ == MetricDataType.Template:
        metric.datatype = MetricDataType.Template
    elif type_ == MetricDataType.Int8Array:
        metric.datatype = MetricDataType.Int8Array
    elif type_ == MetricDataType.Int16Array:
        metric.datatype = MetricDataType.Int16Array
    elif type_ == MetricDataType.Int32Array:
        metric.datatype = MetricDataType.Int32Array
    elif type_ == MetricDataType.Int64Array:
        metric.datatype = MetricDataType.Int64Array
    elif type_ == MetricDataType.UInt8Array:
        metric.datatype = MetricDataType.UInt8Array
    elif type_ == MetricDataType.UInt16Array:
        metric.datatype = MetricDataType.UInt16Array
    elif type_ == MetricDataType.UInt32Array:
        metric.datatype = MetricDataType.UInt32Array
    elif type_ == MetricDataType.UInt64Array:
        metric.datatype = MetricDataType.UInt64Array
    elif type_ == MetricDataType.FloatArray:
        metric.datatype = MetricDataType.FloatArray
    elif type_ == MetricDataType.DoubleArray:
        metric.datatype = MetricDataType.DoubleArray
    elif type_ == MetricDataType.BooleanArray:
        metric.datatype = MetricDataType.BooleanArray
    elif type_ == MetricDataType.StringArray:
        metric.datatype = MetricDataType.StringArray
    elif type_ == MetricDataType.DateTimeArray:
        metric.datatype = MetricDataType.DateTimeArray
    else:
        print("Invalid: " + str(type_))

    # Return the metric
    return metric


def getSeqNum():
    """Get the next sequence number."""
    global seqNum
    retVal = seqNum
    # print("seqNum: " + str(retVal))
    seqNum += 1
    if seqNum == 256:
        seqNum = 0
    return retVal


def getBdSeqNum():
    """Get the next birth/death sequence number."""
    global bdSeq
    retVal = bdSeq
    # print("bdSeqNum: " + str(retVal))
    bdSeq += 1
    if bdSeq == 256:
        bdSeq = 0
    return retVal
