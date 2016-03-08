#!/usr/bin/env python
#
# Copyright 2016 leenjewel
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

from __future__ import absolute_import, division, print_function, with_statement

import struct
import json
from pypomelo.message import Message

class Protocol(object) :

    PROTO_TYPE_SYC       = 0x01
    PROTO_TYPE_ACK       = 0x02
    PROTO_TYPE_HEARTBEAT = 0x03
    PROTO_TYPE_DATA      = 0x04
    PROTO_TYPE_FIN       = 0x05

    DICT_VERSION = None
    DICT_ROUTE_TO_CODE = None
    DICT_CODE_TO_ROUTE = None

    PROTOBUF_VERSION = None
    PROTOBUF_SERVER = None
    PROTOBUF_CLIENT = None

    def __init__(self, proto_type, data = ""):
        self.proto_type = proto_type
        self.data = data
        self.length = len(data)


    def head(self):
        return "%s%s"  %(struct.pack("B", self.proto_type), struct.pack(">I", len(self.data))[1:])


    def body(self):
        return self.data


    def append(self, data):
        data_len = len(self.data)
        if data_len >= self.length :
            return False
        need_len = min(self.length - data_len, len(data))
        self.data += data[:need_len]
        return len(self.data) < self.length


    def completed(self) :
        return len(self.data) >= self.length


    def pack(self):
        return "%s%s"  %(self.head(), self.body())


    @classmethod
    def unpack(cls, data):
        head = data[:4]
        proto_type = struct.unpack("B", head[0])[0]
        body_len = struct.unpack(">I", "\x00" + head[1:])[0]
        proto = cls(proto_type, data[4:])
        proto.length = body_len
        return proto


    @classmethod
    def syc(cls, sys_type, sys_version, user_data = {}):
        return cls(cls.PROTO_TYPE_SYC, json.dumps({
            'sys' : {
                'version' : sys_version,
                'type' : sys_type,
            },
            'user' : user_data,
        }))


    @classmethod
    def ack(cls) :
        return cls(cls.PROTO_TYPE_ACK)


    @classmethod
    def heartbeat(cls):
        return cls(cls.PROTO_TYPE_HEARTBEAT)


