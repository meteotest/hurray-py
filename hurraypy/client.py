# Copyright (c) 2016, Meteotest
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Meteotest nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
Hfive Python Client, DB connection interface
"""

import asyncio
import struct

import msgpack

from hurraypy.exceptions import (MessageError, DatabaseError, NodeError,
                                 ServerError)
from hurraypy.log import log
from hurraypy.msgpack_ext import decode, encode
from hurraypy.nodes import File
from hurraypy.protocol import (CMD_CREATE_DATABASE, CMD_KW_STATUS, CMD_KW_DB,
                               CMD_USE_DATABASE, CMD_KW_CMD, CMD_KW_ARGS,
                               CMD_KW_DATA, MSG_LEN, PROTOCOL_VER)


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

    # Read message length (4 bytes) and unpack it into an integer
    raw_msg_length = yield from reader.readexactly(MSG_LEN)
    msg_length = struct.unpack('>I', raw_msg_length)[0]
    log.debug("Handle request (Protocol: v%d, Msg size: %d)",
              protocol_ver, msg_length)

    msg_data = yield from reader.readexactly(msg_length)

    # decode message
    return msgpack.unpackb(msg_data, object_hook=decode,
                           use_list=False, encoding='utf-8')


class Connection:
    """
    Connection to an hfive server and database/file
    """

    def __init__(self, host, port, db=None, unix_socket_path=None):
        """

        :param host:
        :param port:
        :param db:
        :param unix_socket_path:
        """
        self.__loop = asyncio.get_event_loop()
        self.__host = host
        self.__port = port
        self.__db = db
        self.__reader, self.__writer = self._connect(host, port,
                                                     unix_socket_path)

    def _connect(self, host, port, unix_socket_path):
        if unix_socket_path:
            conn = self.__connect_socket(unix_socket_path)
        else:
            conn = self.__connect_tcp(host, port)
        return self.__loop.run_until_complete(conn)

    @asyncio.coroutine
    def __connect_tcp(self, host, port):
        """
        Create TCP connection
        :param host:
        :param port:
        :return: The reader returned is a StreamReader instance; the writer is
        a StreamWriter instance.
        """
        reader, writer = yield from asyncio.open_connection(host, port)
        return reader, writer

    @asyncio.coroutine
    def __connect_socket(self, socket_path):
        """
        Create UNIX Domain Sockets connection
        :param socket_path:
        :return: The reader returned is a StreamReader instance; the writer is
        a StreamWriter instance.
        """
        reader, writer = yield from asyncio.open_unix_connection(socket_path)
        return reader, writer

    def __enter__(self):
        """
        simple context manager (so we can use 'with Connection() as conn:')
        """
        return self

    def __exit__(self, type, value, tb):
        pass

    def create_file(self, name):
        """
        Create an hdf5 file

        Args:
            name: str, name of the database

        Returns:
            None

        Raises:
            DatabaseError if db already exists
        """
        self.send_rcv(CMD_CREATE_DATABASE, {CMD_KW_DB: name})

    def use_file(self, dbname, mode="w"):
        """
        Use an hdf5 file

        Args:
            dbname: str, name of the database
            mode: 'r' or 'w'

        Returns:
            An instance of the Group class

        Raises:
            DatabaseError if ``dbname`` does not exist
        """
        # TODO implement mode
        self.send_rcv(CMD_USE_DATABASE, {CMD_KW_DB: dbname})
        self.__db = dbname

        return File(conn=self, path='/')

    @asyncio.coroutine
    def __send_rcv(self, cmd, args, data):
        """
        """
        msg = msgpack.packb({
            CMD_KW_CMD: cmd,
            CMD_KW_ARGS: args,
            CMD_KW_DATA: data
        }, default=encode, use_bin_type=True)

        log.debug("Sending %d bytes...", len(msg))
        # Prefix message with protocol version
        self.__writer.write(struct.pack('>I', PROTOCOL_VER))
        # Prefix each message with a 4-byte length (network byte order)
        self.__writer.write(struct.pack('>I', len(msg)))
        # send metadata
        self.__writer.write(msg)

        # receive answer from server
        result = yield from _recv(self.__reader)

        return result

    def _run(self, cmd, args, data):
        send_rcv_coroutine = self.__send_rcv(cmd, args, data)
        return self.__loop.run_until_complete(send_rcv_coroutine)

    def send_rcv(self, cmd, args, data=None):
        """
        Process a request to the server

        Args:
            cmd: command
            args: command arguments
            data: numpy array or None

        Returns:
            Tuple (result, array)
        """
        if CMD_KW_DB not in args:
            args[CMD_KW_DB] = self.__db
        result = self._run(cmd, args, data)
        status = result[CMD_KW_STATUS]

        # Handle errors
        if status >= 200:
            if 200 <= status < 300:
                raise MessageError(status)
            if 300 <= status < 400:
                raise DatabaseError(status)
            if 400 <= status < 500:
                raise NodeError(status)
            if 500 <= status < 600:
                raise ServerError(status)

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

    :param host: hostname or IP address.
    :param port: TCP port.
    :param db: database name.
    :param unix_socket_path: Unix domain socket path.
    :return: database connection.
    """
    return Connection(host, port, db, unix_socket_path)
