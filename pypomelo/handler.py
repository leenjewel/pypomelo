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

class Handler(object) :

    def on_connected(self, client, user_data) :
        raise NotImplementedError()


    def on_recv_data(self, client, proto_type, data) :
        return data


    def on_heartbeat(self, client) :
        raise NotImplementedError()


    def on_response(self, client, route, request, response) :
        raise NotImplementedError()


    def on_push(self, client, route, push_data) :
        raise NotImplementedError()


    def on_disconnect(self, client) :
        raise NotImplementedError()


