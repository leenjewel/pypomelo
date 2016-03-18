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

"""A client of Pomelo

Handshake :

+----------+                    +----------+
+  client  +                    +  server  +
+----------+                    +----------+
     |                               |
     |        handshake reqest       |
     |------------------------------>|
     |                               |
     |       handshake response      |
     |<------------------------------|
     |                               |
     |             ack               |
     |------------------------------>|
     |                               |


Heartbeat :

+----------+                    +----------+
+  client  +                    +  server  +
+----------+                    +----------+
     |                               |
     |        heartbeat reqest       |
     |------------------------------>|
     |                               |
     |       heartbeat response      |
     |<------------------------------|
     |                               |
     |       wait for some times     |
     |                               |
     |        heartbeat reqest       |
     |------------------------------>|
     |                               |
     |       heartbeat response      |
     |<------------------------------|
     |                               |
"""

from __future__ import absolute_import, division, print_function, with_statement

from pypomelo.protocol import Protocol
from pypomelo.message import Message
import json
from socket import *

def loop(client) :
    pass

class Client(object) :
    """Pomelo client
    """

    def __init__(self, handler) :
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.handler = handler
        self.dict_version = None
        self.route_to_code = None
        self.code_to_route = None
        self.proto_version = None
        self.global_server_protos = None
        self.global_client_protos = None
        self.request_id = 1
        self.request_handler = {}
        self.msgid_to_route = {}
        self.is_closing = False


    def connect(self, host, port) :
        """Connect TCP
        Send Pomelo handshake request
        """
        self.socket.connect((host, port))
        self.send(Protocol.syc('socket', '1.1.1').pack())


    def close(self) :
        self.is_closing = True


    def run(self) :
        while not self.is_closing :
            recv_data = self.socket.recv(1024)
            if 0 == len(recv_data) :
                break;
            protocol_pack = Protocol.unpack(recv_data[:4])
            recv_data = recv_data[4:]
            while len(recv_data) < protocol_pack.length :
                recv_data += self.socket.recv(1024)
            if hasattr(self.handler, 'on_recv_data') :
                protocol_body = self.handler.on_recv_data(protocol_pack.proto_type, recv_data)
            protocol_pack.append(protocol_body)
            self.on_protocol(protocol_pack)

        self.socket.close()
        if hasattr(self.handler, 'on_disconnect') :
            self.handler.on_disconnect()


    def send(self, data) :
        if not isinstance(data, bytes) :
            data = bytes(data)
        self.socket.send(data)


    def send_request(self, route, request_data, on_request = None) :
        assert isinstance(request_data, dict), "request data must be dictionary"
        self.msgid_to_route[self.request_id] = route
        self.request_handler[self.request_id] = {
            "request_data" : request_data,
            "handler" : on_request,
        }
        message = Message.request(route, self.request_id, request_data)
        self.request_id += 1
        protocol_pack = Protocol(Protocol.PROTO_TYPE_DATA, message.encode(self.route_to_code, self.global_client_protos))
        self.send(protocol_pack.pack())
        return self.request_id


    def send_notify(self, route, notify_data) :
        assert isinstance(notify_data, dict), "Notify data must be dictionary"
        message = Message.notify(route, notify_data)
        protocol_pack = Protocol(Protocol.PROTO_TYPE_DATA, message.encode(self.route_to_code, self.global_client_protos))
        self.send(protocol_pack.pack())


    def send_heartbeat(self) :
        self.send(Protocol.heartbeat().pack())


    def on_protocol(self, protocol_pack) :
        if Protocol.PROTO_TYPE_SYC == protocol_pack.proto_type :
            protocol_data = protocol_pack.data
            message = json.loads(protocol_data[protocol_data.find('{') : protocol_data.rfind('}')+1])
            if 200 == message['code'] :
                sys = message['sys']
                if sys.get('useDict', False) :
                    self.dict_version = sys['dictVersion']
                    self.route_to_code = sys['routeToCode']
                    self.code_to_route = sys['codeToRoute']
                if sys.get('useProto', False) :
                    sys_protos = sys['protos']
                    self.proto_version = sys_protos['version']
                    self.global_server_protos = sys_protos['server']
                    self.global_client_protos = sys_protos['client']
                self.send(Protocol.ack().pack())
                if hasattr(self.handler, 'on_connected') :
                    self.handler.on_connected(self, message.get('user'))
        elif Protocol.PROTO_TYPE_HEARTBEAT == protocol_pack.proto_type :
            if hasattr(self.handler, 'on_heartbeat') :
                self.handler.on_heartbeat()
        elif Protocol.PROTO_TYPE_FIN == protocol_pack.proto_type :
            self.close()
        elif Protocol.PROTO_TYPE_DATA == protocol_pack.proto_type :
            protocol_data = protocol_pack.data
            message = Message.decode(self.code_to_route, self.global_server_protos, protocol_data, self.msgid_to_route)
            if Message.MSG_TYPE_RESPONSE == message.msg_type :
                msg_id = message.msg_id
                request_handler = self.request_handler.get(msg_id)
                if request_handler :
                    route = self.msgid_to_route.get(msg_id)
                    request = request_handler['request_data']
                    handler = request_handler.get('handler')
                    if handler is None :
                        if hasattr(self.handler, 'on_response') :
                            self.handler.on_response(self, route, request, message.body)
                    else :
                        handler(self, route, request, message.body)
                del self.msgid_to_route[msg_id]
                del self.request_handler[msg_id]
            elif Message.MSG_TYPE_PUSH == message.msg_type :
                if hasattr(self.handler, 'on_notify') :
                    route = message.route
                    self.handler.on_notify(self, route, message.body)


