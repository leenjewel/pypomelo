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

import struct

class Stream(object) :

    def __init__(self, data = "") :
        self.index = 0
        self.data = data
        self.size = len(self.data)


    def tell(self) :
        return self.index


    def seek(self, seek) :
        seek = max(0, seek)
        self.index = min(self.size, seek)


    def read(self, size = None) :
        if self.size <= self.index :
            return ''
        if size is None :
            size = self.size - self.index
        start = self.index
        end = min(self.size, self.index + size)
        self.index = end
        return (self.data[start : end])


    def write(self, data) :
        self.data += data
        self.size = len(self.data)


    def getvalue(self) :
        return self.data

