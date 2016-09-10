"""
Hfive Python Client, DB connection interface
"""

import asyncio
import struct

import msgpack

from hurraypy.const import FILE_EXISTS, FILE_NOTFOUND
from hurraypy.msgpack_numpy import decode_np_array, encode_np_array
from hurraypy.nodes import Group

# 4 bytes are used to encode message lengths
MSG_LEN = 4
PROTOCOL_VER = 1


def _recv(reader):
    """
    Receive and decode message

    Args:
        reader: StreamReader object

    Returns:
        Tuple (result, array), where result is a dict and array is either a
        numpy array or None.
    """
    # read protocol version
    protocol_ver = yield from reader.readexactly(MSG_LEN)
    protocol_ver = struct.unpack('>I', protocol_ver)[0]
    # print("protocol version:", protocol_ver)

    # Read message length (4 bytes) and unpack it into an integer
    raw_msg_length = yield from reader.readexactly(MSG_LEN)
    msg_length = struct.unpack('>I', raw_msg_length)[0]
    # print("message size: {}".format(no_bytes))

    msg_data = yield from reader.readexactly(msg_length)

    # decode message
    return msgpack.unpackb(msg_data, object_hook=decode_np_array, use_list=False)


class Connection:
    """
    Connection to an hfive server and database/file
    """

    def __init__(self, host, port, db=None, unix_socket_path=None):
        """
        Args:
            host: host name or IP address
            port: TCP port
            db: name of database or None
        """
        self.__loop = asyncio.get_event_loop()
        self.__host = host
        self.__port = port
        self.__db = db
        if unix_socket_path:
            self.__reader, self.__writer = self.__loop.run_until_complete(self.__connect_socket(unix_socket_path))
        else:
            self.__reader, self.__writer = self.__loop.run_until_complete(self.__connect_tcp(host, port))

    @asyncio.coroutine
    def __connect_tcp(self, host, port):
        reader, writer = yield from asyncio.open_connection(host, port)
        return reader, writer

    @asyncio.coroutine
    def __connect_socket(self, unix_socket_path):
        reader, writer = yield from asyncio.open_unix_connection(unix_socket_path)
        return reader, writer

    def __enter__(self):
        """
        simple context manager (so we can use 'with Connection() as conn:')
        """
        return self

    def __exit__(self, type, value, tb):
        pass

    def create_db(self, name):
        """
        Create a database / hdf5 file

        Args:
            name: str, name of the database

        Returns:
            None

        Raises:
            OSError if db already exists
        """
        result = self.send_rcv('create_db', {'name': name})

        if result[b'status'] == FILE_EXISTS:
            raise OSError('db exists')

    def connect_db(self, dbname):
        """
        Connect to database

        Args:
            dbname: str, name of the database

        Returns:
            An instance of the Group class

        Raises:
            ValueError if ``dbname`` does not exist
        """
        result = self.send_rcv('connect_db', {'name': dbname})

        if result[b'status'] == FILE_NOTFOUND:
            raise ValueError('db not found')
        else:
            self.__db = dbname
            return Group(self, '/')

    def send_rcv(self, cmd, args, arr=None):
        """
        Process a request to the server

        Args:
            cmd: command
            args: command arguments
            arr: numpy array or None

        Returns:
            Tuple (result, array)
        """
        if 'db' not in args:
            args['db'] = self.__db
        send_rcv_coroutine = self.__send_rcv(cmd, args, arr)
        result = self.__loop.run_until_complete(send_rcv_coroutine)

        return result

    @asyncio.coroutine
    def __send_rcv(self, cmd, args, arr):
        """
        """
        data = msgpack.packb({
            'cmd': cmd,
            'args': args,
            'arr': arr
        }, default=encode_np_array)

        # print("Sending {} bytes...".format(msg_len))
        # Prefix message with protocol version
        self.__writer.write(struct.pack('>I', PROTOCOL_VER))
        # Prefix each message with a 4-byte length (network byte order)
        self.__writer.write(struct.pack('>I', len(data)))
        # send metadata
        self.__writer.write(data)

        # receive answer from server
        # print("receiving answer from server...", flush=True)
        result = yield from _recv(self.__reader)

        return result

    @property
    def db(self):
        """
        wrapper
        """
        return self.__db


def connect(host='localhost', port=2222, db=None, unix_socket_path=None):
    """
    Creates and returns a database connection object.

    Args:
        host: str, hostname or IP address
        port: int, TCP port
        db: database name

    Returns:
        An instance of the Connection class
    """
    return Connection(host, port, db, unix_socket_path)
