#!/usr/bin/env python
#
# Copyright 2019 leenjewel
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Protobuf encoder and decoder

About Protobuf see :
https://developers.google.com/protocol-buffers/

About protobuf of pomelo see :
https://github.com/NetEase/pomelo/wiki/Communication-Protocol

About protobuf varints see :
https://developers.google.com/protocol-buffers/docs/encoding#varints

Pomelo Server define protobuf protocol on configure file clientProtos.json :

{
    "connector.entryHandler.test" : {
        "required uInt32 test_uInt32" : 1,
        "required int32 test_int32" : 2,
        "required sInt32 test_sInt32" : 3,
        "required float test_float" : 4,
        "required double test_double" : 5,
        "required string test_string" : 6,
        "repeated int32 test_repeated" : 7,
        "message Test_submessage" : {
            "required uInt32 sub_uInt32" : 1,
            "required int32 sub_int32" : 2,
            "required sInt32 sub_sInt32" : 3,
            "required float sub_float" : 4,
            "required double sub_double" : 5,
            "required string sub_string" : 6,
            "repeated int32 sub_repeated" : 7
        },
        "required Test_submessage test_submessage" : 8
    }
}

And change to JSON data send to client at handshake :

{
    "connector.entryHandler.test": {
        "__tags": {
            "1": "test_uInt32",
            "2": "test_int32",
            "3": "test_sInt32",
            "4": "test_float",
            "5": "test_double",
            "6": "test_string",
            "7": "test_repeated",
            "8": "test_submessage"
        },
        "test_uInt32":     {"option": "required", "tag": 1, "type": "uInt32"},
        "test_int32":      {"option": "required", "tag": 2, "type": "int32"},
        "test_sInt32":     {"option": "required", "tag": 3, "type": "sInt32"},
        "test_float":      {"option": "required", "tag": 4, "type": "float"},
        "test_double":     {"option": "required", "tag": 5, "type": "double"},
        "test_string":     {"option": "required", "tag": 6, "type": "string"},
        "test_repeated":   {"option": "repeated", "tag": 7, "type": "int32"},
        "test_submessage": {"option": "required", "tag": 8, "type": "Test_submessage"},
        "__messages": {
            "Test_submessage": {
                "__messages": {},
                "__tags": {
                    "1": "sub_uInt32",
                    "2": "sub_int32",
                    "3": "sub_sInt32",
                    "4": "sub_float",
                    "5": "sub_double",
                    "6": "sub_string",
                    "7": "sub_repeated"
                },
                "sub_uInt32":     {"option": "required", "tag": 1, "type": "uInt32"},
                "sub_int32":      {"option": "required", "tag": 2, "type": "int32"},
                "sub_sInt32":     {"option": "required", "tag": 3, "type": "sInt32"},
                "sub_float":      {"option": "required", "tag": 4, "type": "float"},
                "sub_double":     {"option": "required", "tag": 5, "type": "double"},
                "sub_string":     {"option": "required", "tag": 6, "type": "string"},
                "sub_repeated":   {"option": "repeated", "tag": 7, "type": "int32"}
            }
        }
    }
}

"""

from __future__ import absolute_import, division, print_function, with_statement

import struct
from pypomelo.stream import Stream

PROTOBUF_UINT32 = 1
PROTOBUF_INT32  = 2
PROTOBUF_SINT32 = 3
PROTOBUF_FLOAT  = 4
PROTOBUF_DOUBLE = 5
PROTOBUF_STRING = 6

def protobuf_encode_varint(value):
    """Encode uInt32 and int32

    About protobuf varints see :
    https://developers.google.com/protocol-buffers/docs/encoding#varints
    """
    value = int(value)
    buf = []
    if 0 == value :
        return value
    while value :
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf[-1] &= 0x7F
    return struct.pack("{0}B".format(len(buf)), *buf)


def protobuf_encode_svarint(value):
    if value < 0 :
        return protobuf_encode_varint(~(value << 1))
    else :
        return protobuf_encode_varint(value << 1)


def protobuf_encode_fixed32(value, bigend = False) :
    if bigend :
        return struct.pack(">f", float(value))
    return struct.pack("f", float(value))


def protobuf_encode_fixed64(value, bigend = False) :
    if bigend :
        return struct.pack(">d", float(value))
    return struct.pack("d", float(value))


def protobuf_encode_string(value) :
    value = str(value)
    value_len = len(value)
    pack_value = struct.pack("{0}s".format(value_len), value)
    return protobuf_encode_varint(len(pack_value)) + pack_value


def protobuf_get_type(proto_type) :
    if 'uInt32' == proto_type :
        return PROTOBUF_UINT32
    if 'int32' == proto_type :
        return PROTOBUF_INT32
    if 'sInt32' == proto_type :
        return PROTOBUF_SINT32
    if 'float' == proto_type :
        return PROTOBUF_FLOAT
    if 'double' == proto_type :
        return PROTOBUF_DOUBLE
    if 'string' == proto_type :
        return PROTOBUF_STRING
    return 0


def protobuf_get_constant_type(proto_type) :
    """About protobuf write types see :
    https://developers.google.com/protocol-buffers/docs/encoding#structure

    +--------------------------------------+
    + Type + Meaning +       Used For      +
    +--------------------------------------+
    +      +         + int32, int64, uint32+
    +  0   + Varint  + uint64,sint32,sint64+
    +      +         + boolean, enum       +
    +--------------------------------------+
    +      +         +                     +
    +  1   + 64-bit  + fixed64, sfixed64,  +
    +      +         + double              +
    +--------------------------------------+
    +  2   + string  + string              +
    +--------------------------------------+
    +  5   + 32-bit  + float               +
    +--------------------------------------+
    """
    if 'uInt32' == proto_type or \
        'sInt32' == proto_type or \
        'int32' == proto_type :
            return 0
    elif 'double' == proto_type :
        return 1
    elif 'string' == proto_type :
        return 2
    elif 'float' == proto_type :
        return 5
    return 2


def protobuf_encode_tag(proto_writetype, proto_tag) :
    tag = proto_writetype | (proto_tag << 3)
    return protobuf_encode_varint(tag)


def protobuf_encode_tag_for_field(proto) :
    proto_type = proto['type']
    proto_tag = proto['tag']
    proto_writetype = protobuf_get_constant_type(proto_type)
    return protobuf_encode_tag(proto_writetype, int(proto_tag))


def protobuf_encode_submessage(global_protos, protos, value) :
    data = protobuf_encode(global_protos, protos, value)
    ret = protobuf_encode_varint(len(data))
    ret += data
    return ret


def protobuf_encode_proto(global_protos, protos, proto, value) :
    proto_type_key = (proto['type'])
    proto_type = protobuf_get_type(proto_type_key)
    if PROTOBUF_UINT32 == proto_type :
        return protobuf_encode_varint(value)
    elif PROTOBUF_INT32 == proto_type or PROTOBUF_SINT32 == proto_type :
        return protobuf_encode_svarint(value)
    elif PROTOBUF_FLOAT == proto_type :
        return protobuf_encode_fixed32(value)
    elif PROTOBUF_DOUBLE == proto_type :
        return protobuf_encode_fixed64(value)
    elif PROTOBUF_STRING == proto_type :
        return protobuf_encode_string(value)
    else :
        proto_messages = protos.get('__messages')
        if proto_messages :
            sub_message = proto_messages.get(proto_type_key)
            if None == sub_message :
                sub_message = global_protos.get("message "+proto_type_key)
            if sub_message :
                return protobuf_encode_submessage(global_protos, sub_message, value)
    return ""


def protobuf_encode_array(global_protos, protos, proto, value) :
    ret = ""
    proto_type = protobuf_get_type(proto['type'])
    if proto_type > 0 and proto_type != PROTOBUF_STRING :
        ret += protobuf_encode_tag_for_field(proto)
        ret += protobuf_encode_varint(len(value))
        for val in value :
            ret += protobuf_encode_proto(global_protos, protos, proto, val)
    else :
        for val in value :
            ret += protobuf_encode_tag_for_field(proto)
            ret += protobuf_encode_proto(global_protos, protos, proto, val)
    return ret


def protobuf_encode(global_protos, protos, data) :
    ret = ""
    for key, value in data.items() :
        proto = protos.get(key)
        if not proto :
            continue
        option = proto['option']
        if 'required' == option or 'optional' == option :
            ret += protobuf_encode_tag_for_field(proto)
            ret += protobuf_encode_proto(global_protos, protos, proto, value)
        elif 'repeated' == option :
            if isinstance(value, list) or isinstance(value, tuple) :
                ret += protobuf_encode_array(global_protos, protos, proto, value)
    return ret


def protobuf_decode_varint(stream):
    ret = 0
    bitpos = 0
    while True :
        if bitpos >= 64 :
            break
        byte = stream.read(1)
        if len(byte) == 0 :
            break
        byte = struct.unpack("B", byte)[0]
        ret |= ((byte & 0x7F) << bitpos)
        bitpos += 7
        if (byte & 0x80) == 0 :
            return ret
    raise ValueError('varint overflow')


def protobuf_decode_svarint(stream):
    ret = protobuf_decode_varint(stream)
    if ret & 1 :
        return (~(ret >> 1))
    else :
        return (ret >> 1)


def protobuf_decode_fixed32(stream, bigend = False):
    if bigend :
        return struct.unpack('>f', stream.read(4))[0]
    return struct.unpack('f', stream.read(4))[0]


def protobuf_decode_fixed64(stream, bigend = False):
    if bigend :
        return struct.unpack('>d', stream.read(8))[0]
    return struct.unpack('d', stream.read(8))[0]


def protobuf_decode_string(stream, size) :
    return stream.read(size).decode('utf8')


def protobuf_decode_tag(stream) :
    eof = 0
    writetype = 0
    tag = 0
    try :
        temp = protobuf_decode_varint(stream)
    except ValueError, e:
        tag = -1
        if stream.tell() == 0 :
            eof = 1
        return tag, writetype, eof
    if 0 == temp :
        eof = 1
        return tag, writetype, eof
    tag = temp >> 3
    writetype = (temp & 0x7)
    return tag, writetype, eof


def protobuf_decode_submessage(stream, global_protos, protos, result) :
    size = protobuf_decode_varint(stream)
    substream = Stream(stream.read(size))
    return protobuf_decode(substream, global_protos, protos, result)


def protobuf_decode_proto(stream, global_protos, protos, proto, proto_tag, result) :
    proto_type_key = proto['type']
    proto_type = protobuf_get_type(proto_type_key)
    value = None
    if PROTOBUF_UINT32 == proto_type :
        value = protobuf_decode_varint(stream)
    elif PROTOBUF_INT32 == proto_type or PROTOBUF_SINT32 == proto_type :
        value = protobuf_decode_svarint(stream)
    elif PROTOBUF_FLOAT == proto_type :
        value = protobuf_decode_fixed32(stream)
    elif PROTOBUF_DOUBLE == proto_type :
        value = protobuf_decode_fixed64(stream)
    elif PROTOBUF_STRING == proto_type :
        str_len = protobuf_decode_varint(stream)
        value = protobuf_decode_string(stream, str_len)

    if isinstance(result, dict) :
        result[proto_tag] = value
    elif isinstance(result, list) :
        result.append(value)

    if None is value :
        proto_messages = protos.get('__messages')
        if proto_messages :
            sub_message = proto_messages.get(proto_type_key)
            if None == sub_message :
                sub_message = global_protos.get("message "+proto_type_key)
            if sub_message :
                if proto_tag is None or len(proto_tag) == 0 :
                    protobuf_decode_submessage(stream, global_protos, sub_message, result)
                else :
                    sub_value = {}
                    protobuf_decode_submessage(stream, global_protos, sub_message, sub_value)
                    result[proto_tag] = sub_value


def protobuf_decode_array(stream, global_protos, protos, proto, proto_tag, result) :
    array = []
    proto_type_key = proto['type']
    proto_type = protobuf_get_type(proto_type_key)
    if proto_type > 0 and PROTOBUF_STRING != proto_type :
        size = protobuf_decode_varint(stream)
        for i in range(size) :
            protobuf_decode_proto(stream, global_protos, protos, proto, proto_tag, array)
    elif proto_type > 0 and PROTOBUF_STRING == proto_type :
        protobuf_decode_proto(stream, global_protos, protos, proto_tag, array)
    else:
        value = {}
        protobuf_decode_proto(stream, global_protos, protos, proto, None, value)
        array.append(value)
    result[proto_tag] = array


def protobuf_decode(stream, global_protos, protos, result = {}) :
    while True :
        tag, writetype, eof = protobuf_decode_tag(stream)
        if -1 == tag :
            if eof > 0 :
                break
            else:
                return result
        tags = protos['__tags']
        proto_tag = tags[u''+str(tag)]
        proto = protos[proto_tag]
        option = proto['option']
        if 'optional' == option or 'required' == option :
            protobuf_decode_proto(stream, global_protos, protos, proto, proto_tag, result)
        elif 'repeated' == option :
            protobuf_decode_array(stream, global_protos, protos, proto, proto_tag, result)


