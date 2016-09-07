from __future__ import print_function

import os

import numpy as np
import resource

from tornado.ioloop import IOLoop
from tornado import gen
from tornado.tcpclient import TCPClient
from tornado.options import options, define

print(os.getpid())

define("host", default="localhost", help="TCP server host")
define("port", default=8888, help="TCP port to connect to")


@gen.coroutine
def send_message():
    stream = yield TCPClient().connect(options.host, options.port)
    start_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    data = np.random.randint(0, 255, dtype='u1', size=10)

    yield np.save(stream, data)
    yield stream.write(b'\n')

    view = memoryview(data).cast('B')

    #WRITE_BUFFER_CHUNK_SIZE = 128 * 1024

    #for i in range(0, len(data), WRITE_BUFFER_CHUNK_SIZE):
    #    print("WAS", data[i:i + WRITE_BUFFER_CHUNK_SIZE])
    #    yield stream.write(data[i:i + WRITE_BUFFER_CHUNK_SIZE].data)

    # Do dumb copy
    data1 = data.tostring()

    end_mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    print("Memory: %d" % (end_mem - start_mem))


    yield stream.write(b'sdfadsf')
    reply = yield stream.read_until(b"\n")
    print("Response from server:", reply.decode().strip())


if __name__ == "__main__":
    options.parse_command_line()
    IOLoop.current().run_sync(send_message)
