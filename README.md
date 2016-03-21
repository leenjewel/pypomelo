#PyPomelo

pomelo protocol by python

##Usage

```python

import sys
from pypomelo.tornadoclient import TornadoClient as Client
from pypomelo.handler import Handler
import tornado.ioloop

if __name__ == '__main__' :
    if len(sys.argv) < 3 :
        print "usage python %s host port" %(sys.argv[0])
        sys.exit(1)

    class ClientHandler(Handler) :

        def on_recv_data(self, client, proto_type, data) :
            return data

        def on_connected(self, client, user_data) :
            client.send_heartbeat()
        
        def on_heartbeat(self, client) :
            req_data = {
                "test_uInt32" : 100,
                "test_int32" : -100,
                "test_sInt32" : 200,
                "test_float" : 300.3,
                "test_double" : 400.4,
                "test_string" : "test string",
                "test_repeated" : [5,4,3,2,1],
                "test_submessage" : {
                    "test_uInt32" : 10,
                    "test_int32" : -10,
                    "test_sInt32" : 20,
                    "test_float" : 30.3,
                    "test_double" : 40.4,
                    "test_string" : "sub test string",
                    "test_repeated" : [50,40,30,20,10],
                }
            }
            client.send_request("connector.entryHandler.test", req_data)

        def on_response(self, client, route, request, response) :
            print "response..."
            print response

        def on_push(self, client, route, push_data) :
            print "push..."
            print route, " = ", push_data
        
        def on_disconnect(self, client) :
            print "disconnect..."

    host = sys.argv[1]
    port = sys.argv[2]
    #Start some client
    for i in range(10) :
        handler = ClientHandler()
        client = Client(handler)
        client.connect(host, int(port))
    tornado.ioloop.IOLoop.current().start()
```