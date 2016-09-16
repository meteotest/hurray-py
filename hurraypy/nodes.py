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
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
Hdf5 entities (Nodes, Groups, Datasets)
"""

import os

from hurraypy.protocol import CMD_KW_STATUS, RESPONSE_NODE_TYPE, NODE_TYPE_GROUP, NODE_TYPE_DATASET, \
    RESPONSE_NODE_SHAPE, \
    RESPONSE_NODE_DTYPE, CMD_GET_NODE, CMD_CREATE_DATASET, RESPONSE_DATA, CMD_ATTRIBUTES_SET, CMD_ATTRIBUTES_CONTAINS, \
    RESPONSE_ATTRS_CONTAINS, CMD_ATTRIBUTES_GET, RESPONSE_ATTRS_KEYS, CMD_ATTRIBUTES_KEYS, CMD_KW_PATH, \
    CMD_CREATE_GROUP, \
    CMD_KW_KEY, CMD_SLICE_DATASET
from hurraypy.status_codes import OK, GROUP_EXISTS, DATASET_EXISTS


class Node(object):
    """
    HDF5 node
    """

    def __init__(self, conn, path):
        """
        Args:
            conn: Connection object
            path: full path to the hdf5 node
        """
        self._conn = conn
        self._path = path
        # every node has an attrs property
        self.attrs = AttributeManager(self._conn, self._path)

    def _compose_path(self, name):
        """
        """
        if name.startswith('/'):  # absolute path
            return name
        else:  # relative path
            return os.path.join(self.path, name)

    def __getitem__(self, key):
        """
        Args:
            key: hdf5 path

        Returns:
            An instance of Node (or of a subclass).

        Raises:
            KeyError if object does not exist.
        """
        # note that class Dataset overrides this method

        path = self._compose_path(key)
        args = {
            CMD_KW_PATH: path,
        }
        result = self._conn.send_rcv(CMD_GET_NODE, args)

        if result[CMD_KW_STATUS] == OK:
            if result[RESPONSE_NODE_TYPE] == NODE_TYPE_GROUP:
                return Group(self._conn, path)
            elif result[RESPONSE_NODE_TYPE] == NODE_TYPE_DATASET:
                shape = tuple(result[RESPONSE_NODE_SHAPE])  # compatibility with numpy
                dtype = result[RESPONSE_NODE_DTYPE]
                return Dataset(self._conn, path, shape=shape, dtype=dtype)
            else:
                raise RuntimeError("server returned unknown node type")
        else:
            # TODO error handling
            raise KeyError("%d: could not get item" % result[CMD_KW_STATUS])

    @property
    def path(self):
        """
        wrapper
        """
        return self._path


class Group(Node):
    """
    HDF5 group
    """

    def __init__(self, conn, path):
        Node.__init__(self, conn, path)

    def __repr__(self):
        return "<HDF5 Group (db={}, path={})>".format(self._conn.db,
                                                      self._path)

    def create_group(self, name):
        """
        Args:
            name: name or path of the group, may contain slashes, e.g.,
                'group/subgroup'

        Raises:
            ValueError if group already exists
        """
        group_path = self._compose_path(name)
        args = {
            CMD_KW_PATH: group_path,
        }
        result = self._conn.send_rcv(CMD_CREATE_GROUP, args)
        if result[CMD_KW_STATUS] == OK:
            return Group(self._conn, group_path)
        elif result[CMD_KW_STATUS] == GROUP_EXISTS:
            raise ValueError("Group already exists")

    def create_dataset(self, name, data=None, shape=None, init_value=0,
                       dtype=None, attrs=None):
        """
        Provide either ``data`` or both ``shape`` and ``init_value``.

        Args:
            name: name or path of the dataset
            data: numpy array
            shape: tuple denoting the shape of the array to be created
            init_value: initial value to be used to create array. Possible
                values: either a scaler (int, float) or 'random'
            dtype: if ``init_value`` is 'random', you can optionally provide
                a dtype.
            attrs: dictionary of attributes TODO

        Raises:
            ValueError is dataset already exists
        """
        dst_path = self._compose_path(name)
        if data is None:
            args = {
                CMD_KW_PATH: dst_path,
                'shape': shape,
                'init_value': init_value,
                'dtype': dtype
            }
        else:
            args = {
                CMD_KW_PATH: dst_path,
            }
        result = self._conn.send_rcv(CMD_CREATE_DATASET, args, data)

        if result[CMD_KW_STATUS] == DATASET_EXISTS:
            raise ValueError("dataset already exists")
        else:
            return Dataset(self._conn, dst_path, shape=shape, dtype=dtype)

    def require_dataset(self, **kwargs):
        raise NotImplementedError()

    def keys(self):
        raise NotImplementedError()

    def items(self):
        """
        """
        raise NotImplementedError()

    def __contains__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()


class Dataset(Node):
    """
    Wrapper for h5py.Dataset
    """

    def __init__(self, conn, path, shape, dtype):
        Node.__init__(self, conn, path)
        self.__shape = shape
        self.__dtype = dtype

    def __getitem__(self, key):
        """
        Multidimensional slicing for datasets

        Args:
            key: key object, e.g., slice() object

        Returns:
            Numpy array

        Raises:
            IndexError if ``key`` was illegal
        """
        # TODO check if dtype corresponds to self.dtype (dataset may have been
        # overwritten in the meantime)
        args = {
            CMD_KW_PATH: self.path,
            CMD_KW_KEY: key
        }
        result = self._conn.send_rcv(CMD_SLICE_DATASET, args)

        if result[CMD_KW_STATUS] == OK:
            return result[RESPONSE_DATA]
        else:
            # TODO error handling
            raise IndexError("could not get data")

    def __setitem__(self, key, value):
        """
        Broadcasting for datasets. Example: mydataset[0,:] = np.arange(100)
        """
        args = {
            CMD_KW_PATH: self.path,
            CMD_KW_KEY: key,
        }
        result = self._conn.send_rcv('broadcast_dataset', args, value)

        if result[CMD_KW_STATUS] != OK:
            # TODO error handling
            raise ValueError("operation failed: {}".format(result[CMD_KW_STATUS]))

    @property
    def shape(self):
        """
        Returns:
            a shape tuple
        """
        return self.__shape

    @property
    def dtype(self):
        """
        Returns:
            numpy dtype
        """
        return self.__dtype


class AttributeManager(object):
    """
    Provides same features as AttributeManager from h5py.
    """

    def __init__(self, conn, path):
        """
        Args:
            conn: Connection object
            path: full path to hdf5 node
        """
        self.__conn = conn
        self.__path = path

    def __iter__(self):
        raise NotImplementedError()

    def keys(self):
        """
        Returns attribute keys (list)
        """
        args = {
            CMD_KW_PATH: self.__path,
        }
        result = self.__conn.send_rcv(CMD_ATTRIBUTES_KEYS, args)

        if result[CMD_KW_STATUS] == OK:
            return result[RESPONSE_ATTRS_KEYS]
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __contains__(self, key):
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
        }
        result = self.__conn.send_rcv(CMD_ATTRIBUTES_CONTAINS, args)
        if result[CMD_KW_STATUS] == OK:
            return result[RESPONSE_ATTRS_CONTAINS]
        else:
            raise RuntimeError("Error")

    def __getitem__(self, key):
        """
        Get attribute value for given ``key``.

        Returns:
            a primitive object (string, number) or a numpy array.
        """
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
        }
        result = self.__conn.send_rcv(CMD_ATTRIBUTES_GET, args)
        arr = result[RESPONSE_DATA]
        if result[CMD_KW_STATUS] == OK:
            return arr if arr is not None else result[RESPONSE_DATA]
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __setitem__(self, key, value):
        """
        Set/overwrite attribute ``key`` with given ``value`` (scalar, string,
        or numpy array).
        """
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
        }

        result = self.__conn.send_rcv(CMD_ATTRIBUTES_SET, args, value)

        if result[CMD_KW_STATUS] == OK:
            pass
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def __delitem__(self, key):
        raise NotImplementedError()

    def get(self, key, defaultvalue):
        """
        Return attribute value or return a default value if key is missing.

        Args:
            key: attribute key
            defaultvalue: default value to be returned if key is missing
        """
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
            'default': defaultvalue,
        }
        result = self.__conn.send_rcv('attrs_get', args)
        arr = result[RESPONSE_DATA]
        if result[CMD_KW_STATUS] == OK:
            return arr if arr is not None else result['value']
        else:
            # TODO error handling
            raise RuntimeError("Error")

    def to_dict(self):
        """
        Return attributes as dict
        """
        raise NotImplementedError()