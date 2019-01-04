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

from __future__ import absolute_import, division, print_function, with_statement

from tornado.iostream import IOStream
from tornado.ioloop import IOLoop
from socket import *
from pypomelo.client import Client
from pypomelo.protocol import Protocol

class TornadoClient(Client) :
    """A non-blocking Pomelo client by tornado ioloop

    Usage :

        class ClientHandler(object) :

            def on_recv_data(self, client, proto_type, data) :
                print "recv_data..."
                return data

            def on_connected(self, client, user_data) :
                print "connect..."
                client.send_heartbeat()

            def on_disconnect(self, client) :
                print "disconnect..."

            def on_heartbeat(self, client) :
                print "heartbeat..."
                send request ...

            def on_response(self, client, route, request, response) :
                print "response..."

            def on_push(self, client, route, push_data) :
                print "notify..."

        handler = ClientHandler()
        client = TornadoClient(handler)
        client.connect(host, int(port))
        client.run()
        tornado.ioloop.IOLoop.current().start()
    """

    def __init__(self, handler) :
        self.socket = socket(AF_INET, SOCK_STREAM)
        self.iostream = None
        self.protocol_package = None
        super(TornadoClient, self).__init__(handler)

    def connect(self, host, port) :
        self.iostream = IOStream(self.socket)
        self.iostream.set_close_callback(self.on_close)
        self.iostream.connect((host, port), self.on_connect)


    def on_connect(self) :
        self.send_sync()
        self.on_data()


    def on_close(self) :
        if hasattr(self.handler, 'on_disconnect') :
            self.handler.on_disconnect(self)


    def send(self, data) :
        assert not self.iostream.closed(), "iostream has closed"
        if not isinstance(data, bytes) :
            data = bytes(data)
        self.iostream.write(data)


    def on_data(self) :
        assert not self.iostream.closed(), "iostream has closed"
        if None is self.protocol_package or self.protocol_package.completed() :
            self.iostream.read_bytes(4, self.on_head)


    def on_head(self, head) :
        self.protocol_package = Protocol.unpack(head)
        self.iostream.read_bytes(self.protocol_package.length, self.on_body)


    def on_body(self, body) :
        if hasattr(self.handler, 'on_recv_data') :
            body = self.handler.on_recv_data(self, self.protocol_package.proto_type, body)
        self.protocol_package.append(body)
        self.on_protocol(self.protocol_package)
        self.on_data()


    def close(self) :
        if self.iostream :
            self.iostream.close()


