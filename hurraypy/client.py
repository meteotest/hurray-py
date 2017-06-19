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
hurray Python client, connection interface
"""

import socket
import struct

import msgpack

from hurraypy.buffer import Buffer
from hurraypy.exceptions import (MessageError, DatabaseError, NodeError,
                                 ServerError)
from .log import log
from .msgpack_ext import get_decoder, encode
from .nodes import File, Node
from .protocol import (CMD_CREATE_DATABASE, CMD_KW_STATUS, CMD_KW_DB,
                       CMD_KW_OVERWRITE, CMD_USE_DATABASE, CMD_KW_CMD,
                       CMD_KW_ARGS, CMD_KW_DATA, MSG_LEN, PROTOCOL_VER)


class Connection:
    """
    Connection to an hfive server and database/file
    """

    def __init__(self, host, port, unix_socket_path=None, no_delay=True):
        """
        Initialize a connection to a hurray server

        Args:
            host: hostname of IP
            port: TCP port
            unix_socket_path: path to unix domain socket
            no_delay: enabe
        """
        self._host = host
        self._port = port

        if unix_socket_path:
            self.__socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.__socket.connect(unix_socket_path)
        else:
            self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if no_delay:
                self.__socket.setsockopt(socket.IPPROTO_TCP,
                                         socket.TCP_NODELAY, 1)
            self.__socket.connect((host, int(port)))

        self.__buffer = Buffer(self.__socket)

        parent_self = self

        class _File(File):

            def __init__(self, h5file, mode="w"):
                # TODO implement mode
                result = parent_self.send_rcv(CMD_USE_DATABASE,
                                              h5file=h5file, args={})
                # TODO examine result
                File.__init__(self, conn=parent_self, h5file=h5file, path="/")

            def __enter__(self):
                return self

            def __exit__(self, type, value, tb):
                pass

        # TODO explain
        self.File = _File

    def __enter__(self):
        """
        simple context manager (so we can use 'with Connection() as conn:')
        """
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def __repr__(self):
        return "<Connection (host={}, port={})>".format(self._host, self._port)

    def close(self):
        self.__buffer.close()

    def create_file(self, name, overwrite=False):
        """
        Create an hdf5 file

        Args:
            name: str, name of the database
            overwrite: truncate file if it exists

        Returns:
            None

        Raises:
            DatabaseError if db already exists
        """
        args = {
            CMD_KW_OVERWRITE: overwrite,
        }
        result = self.send_rcv(CMD_CREATE_DATABASE, h5file=name, args=args)
        # TODO examine result!

        return File(conn=self, h5file=name, path='/')

    def _recv(self):
        """
        Receive and decode message

        Returns:
            Tuple (result, array), where result is a dict and array is either a
            numpy array or None.
        """
        # read protocol version
        protocol_ver = self.__buffer.read_bytes(MSG_LEN)
        protocol_ver = struct.unpack('>I', protocol_ver)[0]

        # Read message length (4 bytes) and unpack it into an integer
        raw_msg_length = self.__buffer.read_bytes(MSG_LEN)
        msg_length = struct.unpack('>I', raw_msg_length)[0]
        log.debug("Handle request (Protocol: v%d, Msg size: %d)",
                  protocol_ver, msg_length)

        log.debug("Read total of {} bytes ..."
                  .format(2 * MSG_LEN + msg_length))

        msg_data = self.__buffer.read_bytes(msg_length)

        # decode message
        result = msgpack.unpackb(msg_data, object_hook=get_decoder(self),
                                 use_list=False, encoding='utf-8')

        # if result contains a Node => set node.conn = self.conn
        # TODO make this cleaner
        if "data" in result and isinstance(result["data"], Node):
            result["data"].conn = self

        return result

    def __send_rcv(self, cmd, args, data):
        """
        helper for ``send_rcv()``
        """
        msg = msgpack.packb({
            CMD_KW_CMD: cmd,
            CMD_KW_ARGS: args,
            CMD_KW_DATA: data
        }, default=encode, use_bin_type=True)

        log.debug("Sending %d bytes...", len(msg))
        # Prefix message with protocol version
        rsp = struct.pack('>I', PROTOCOL_VER)
        # Prefix each message with a 4-byte length (network byte order)
        rsp += struct.pack('>I', len(msg))
        rsp += msg
        self.__buffer.write(rsp)

        # receive answer from server
        return self._recv()

    def send_rcv(self, cmd, h5file, args, data=None):
        """
        Process a request to the server

        Args:
            h5file: name / relative path of hdf5 file
            cmd: command
            args: command arguments
            data: numpy array or None

        Returns:
            Tuple (result, array)
        """
        if CMD_KW_DB in args:
            raise ValueError("{} must not be in argument 'args'"
                             .format(CMD_KW_DB))
        args[CMD_KW_DB] = h5file

        result = self.__send_rcv(cmd, args, data)

        status = result[CMD_KW_STATUS]

        # Handle errors
        if status >= 200:
            error_msg = result.get(CMD_KW_DATA, "")
            if 200 <= status < 300:
                raise MessageError(status, error_msg)
            if 300 <= status < 400:
                raise DatabaseError(status, error_msg)
            if 400 <= status < 500:
                raise NodeError(status, error_msg)
            if 500 <= status < 600:
                raise ServerError(status, error_msg)

        return result


def connect(host='localhost', port=2222, unix_socket_path=None):
    """
    Creates and returns a database connection object.

    Args:
        host: hostname or IP address
        port: TCP port
        unix_socket_path: Unix domain socket path

    Returns: database connection.
    """
    return Connection(host, port, unix_socket_path)
