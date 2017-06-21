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
Hdf5 entities (Nodes, Groups, Datasets)
"""

import os

from hurraypy.exceptions import NodeError, MessageError
from hurraypy.protocol import (CMD_GET_NODE, CMD_CONTAINS, CMD_CREATE_DATASET,
                               CMD_RENAME_DATABASE, CMD_DELETE_DATABASE,
                               CMD_REQUIRE_DATASET, RESPONSE_DATA,
                               CMD_ATTRIBUTES_SET, CMD_ATTRIBUTES_CONTAINS,
                               CMD_CREATE_GROUP, CMD_REQUIRE_GROUP,
                               RESPONSE_ATTRS_CONTAINS, CMD_ATTRIBUTES_GET,
                               RESPONSE_ATTRS_KEYS, CMD_ATTRIBUTES_KEYS,
                               RESPONSE_NODE_TREE, CMD_KW_PATH,
                               CMD_KW_SHAPE, CMD_KW_DTYPE,
                               CMD_KW_COMPRESSION, CMD_KW_COMPRESSION_OPTS,
                               CMD_KW_CHUNKS, CMD_KW_REQUIRE_EXACT,
                               CMD_KW_FILLVALUE,
                               CMD_KW_KEY,
                               CMD_KW_DB_RENAMETO, CMD_SLICE_DATASET,
                               CMD_BROADCAST_DATASET, CMD_GET_KEYS,
                               CMD_GET_FILESIZE, CMD_GET_TREE,
                               RESPONSE_NODE_KEYS)
from hurraypy.status_codes import KEY_ERROR, NODE_NOT_FOUND, INCOMPATIBLE_DATA
from .ipython import (CSS_TREE, ICON_GROUP, ICON_DATASET, ICON_DATASET_ATTRS,
                      ICON_GROUP_ATTRS, IMG_STYLE)


class Node(object):
    """
    HDF5 node
    """

    def __init__(self, conn, h5file, path):
        """
        Args:
            conn: ``Connection`` object
            h5file: name of hdf5 file this node belongs to
            path: full path to the hdf5 node
        """
        self._conn = conn
        self._h5file = h5file
        self._path = path
        # every node has an attrs property
        self.attrs = AttributeManager(conn=conn, h5file=h5file, path=path)

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, value):
        self._conn = value
        self.attrs.conn = value

    @property
    def h5file(self):
        return self._h5file

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
        path = self._compose_path(key)
        args = {
            CMD_KW_PATH: path,
        }
        try:
            result = self.conn.send_rcv(CMD_GET_NODE, h5file=self.h5file,
                                        args=args)
        except NodeError as e:
            if e.status == NODE_NOT_FOUND:
                raise KeyError("group/dataset not found")
            else:
                raise e

        node = result[RESPONSE_DATA]  # Group or Dataset

        return node

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

    def __init__(self, conn, h5file, path):
        Node.__init__(self, conn, h5file, path)

    def __repr__(self):
        return "<Group (db={}, path={})>".format(self.h5file, self._path)

    def _repr_html_(self):
        """ representation in jupyter notebooks """
        img = ICON_GROUP_ATTRS if len(self.attrs) > 0 else ICON_GROUP
        return ("{}<strong>Group {}</strong> (file={})"
                .format(img, self._path, self.h5file))

    def create_group(self, name):
        """
        Create and return a new group.

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
        self.conn.send_rcv(CMD_CREATE_GROUP, h5file=self.h5file, args=args)
        return Group(conn=self.conn, h5file=self.h5file, path=group_path)

    def require_group(self, name):
        """
        Open/return a group, creating it if it doesn't exist.

        Args:
            name: name or path of the group, may contain slashes, e.g.,
                'group/subgroup'
        """
        group_path = self._compose_path(name)
        args = {
            CMD_KW_PATH: group_path,
        }
        self.conn.send_rcv(CMD_REQUIRE_GROUP, h5file=self.h5file, args=args)
        return Group(conn=self.conn, h5file=self.h5file, path=group_path)

    def create_dataset(self, name, shape=None, dtype=None, data=None,
                       chunks=True, compression=None, compression_opts=None,
                       fillvalue=None):
        """
        Create a new dataset. You can initialize the dataset either with a
        NumPy array (``data``) or with ``shape`` and ``dtype`` arguments.

        If ``shape`` and ``dtype`` are specified along with data, they will
        override ``data.shape`` and ``data.dtype``.

        Examples::

            >>> dset = f.create_dataset("mydst", (200, 400))
            >>> dset = f.create_dataset("mydst2", (200, 400), dtype=np.float64)
            >>> dset = f.create_dataset("mydst3", data=some_numpy_array)

        Args:
            name: name/path of the dataset
            shape: shape of dataset (tuple)
            dtype: data type of dataset
            data: numpy array
            chunks: Chunk shape, or True to enable auto-chunking (default)
            compression: ``None`` (default) or name of compression filter.
                Available filters:
                "gzip": Good compression, moderate speed.  ``compression_opts``
                sets the compression level and may be an integer from 0 to 9,
                default is 4.
                "lzf": Low to moderate compression, very fast. No options.
                "szip": Patent-encumbered filter used in the NASA community.
                Not available with all installations of HDF5 due to legal
                reasons. Consult the HDF5 docs for filter options.
                Cf. http://docs.h5py.org/en/latest/high/dataset.html for more
                details.
            compression_opts: options for compression filter.
            fillvalue: This value will be used when reading uninitialized parts
                of the dataset

        Raises:
            ValueError if dataset already exists, if ``shape`` does not match
            ``data.shape``, or if it is not possible to cast ``data.dtype`` to
            ``dtype``.

        Returns:
            ``Dataset`` object
        """
        if data is None and shape is None:
            raise ValueError("Either 'data' or 'shape' must be specified")

        if compression not in (None, "gzip", "lzf", "szip"):
            raise ValueError("Unknown 'compression' filter: {}"
                             .format(compression))

        dst_path = self._compose_path(name)
        args = {
            CMD_KW_PATH: dst_path,
            CMD_KW_CHUNKS: chunks,
        }
        if shape is not None:
            args[CMD_KW_SHAPE] = shape
        if dtype is not None:
            args[CMD_KW_DTYPE] = dtype
        if compression is not None:
            args[CMD_KW_COMPRESSION] = compression
        if compression_opts is not None:
            args[CMD_KW_COMPRESSION_OPTS] = compression_opts
        if fillvalue is not None:
            args[CMD_KW_FILLVALUE] = fillvalue
        result = self.conn.send_rcv(CMD_CREATE_DATASET, h5file=self.h5file,
                                    args=args, data=data)

        dst = result["data"]  # Dataset

        return dst

    def require_dataset(self, name, shape=None, dtype=None, data=None,
                        chunks=True, compression=None, compression_opts=None,
                        fillvalue=None, exact=False):
        """
        Open a dataset, creating it if it doesn’t exist.

        If keyword ``exact`` is False (default), an existing dataset must have
        the same shape and a conversion-compatible dtype. If True, the shape
        and dtype must match exactly.

        Other dataset keywords (see ``create_dataset()``) may be provided, but
        are only used if a new dataset is to be created.

        Returns:
            ``Dataset`` object

        Raises:
            ``ValueError`` in case of invalid arguments.
            ``MessageError`` if an incompatible object already exists, or if
            the shape or dtype don’t match according to the above rules.
        """
        if data is None and shape is None:
            raise ValueError("Either 'data' or 'shape' must be specified")

        dst_path = self._compose_path(name)
        args = {
            CMD_KW_PATH: dst_path,
            CMD_KW_REQUIRE_EXACT: exact,
            CMD_KW_CHUNKS: chunks,
        }
        if shape is not None:
            args[CMD_KW_SHAPE] = shape
        if dtype is not None:
            args[CMD_KW_DTYPE] = dtype
        if compression is not None:
            args[CMD_KW_COMPRESSION] = compression
        if compression_opts is not None:
            args[CMD_KW_COMPRESSION_OPTS] = compression_opts
        if fillvalue is not None:
            args[CMD_KW_FILLVALUE] = fillvalue
        try:
            result = self.conn.send_rcv(CMD_REQUIRE_DATASET,
                                        h5file=self.h5file, args=args,
                                        data=data)
        except MessageError as e:
            if e.status == INCOMPATIBLE_DATA:
                raise TypeError("shape or dtype incompatible")
            else:
                raise e

        dst = result["data"]  # Dataset

        return dst

    def keys(self):
        args = {
            CMD_KW_PATH: self._path,
        }
        result = self.conn.send_rcv(CMD_GET_KEYS, h5file=self.h5file,
                                    args=args)

        return result[RESPONSE_DATA][RESPONSE_NODE_KEYS]

    def items(self):
        for key in self.keys():
            yield key, self[key]

    def __contains__(self, key):
        args = {
            CMD_KW_PATH: self._path,
            CMD_KW_KEY: key
        }
        result = self.conn.send_rcv(CMD_CONTAINS, h5file=self.h5file,
                                    args=args)

        return result[RESPONSE_DATA]

    def __delitem__(self, key):
        raise NotImplementedError()

    def visititems(self, func):
        """
        Recursively visit all objects in this group and subgroups. You have
        to supply a callable with the signature::

            func(name, object) -> None or return value

        ``object`` will be a ``Group`` or ``Dataset`` instance.
        Return None to continue visiting until all objects are exhausted.
        Returning anything else will immediately stop visiting and return that
        value from visit. Example::

            >>> def find_foo(name, obj):
            ...     ''' Find first object with 'foo' in its name '''
            ...     if 'foo' in name:
            ...         return name
            >>> group.visititems(find_foo)
            'some/subgroup/foo'

        Args:
            func: callable
        """
        # get the whole (sub)tree of nodes from server
        tree = self.tree()

        # traverse tree recursively

        def traverse(treenode):
            node, children = treenode
            func(node.path, node)
            for child in children:
                traverse(child)

        traverse(tree)

    def visit(self, func):
        """
        Like ``visititems()`` but ``func`` expects a callable with just one
        argument::

            func(name) -> None or return value
        """
        tree = self.tree()

        def traverse(treenode):
            node, children = treenode
            func(node.path)
            for child in children:
                traverse(child)

        traverse(tree)

    def tree(self):
        """
        Return tree data structure consisting of all groups and datasets.
        A tree node is defined recursively as a tuple:

            [Dataset/Group, [children]]

        Returns: list
        """
        args = {
            CMD_KW_PATH: self._path,
        }
        result = self.conn.send_rcv(CMD_GET_TREE, h5file=self.h5file,
                                    args=args)
        tree = result[RESPONSE_DATA][RESPONSE_NODE_TREE]

        return Tree(tree)


class File(Group):
    """
    File object
    """

    # def __enter__(self):
    #     return self

    # def __exit__(self, *args):
    #     pass

    def _repr_html_(self):
        """ representation in jupyter notebooks """
        txt = ("<strong>File {}</strong>"
               .format(self.h5file))
        icon = ICON_GROUP_ATTRS if len(self.attrs) > 0 else ICON_GROUP
        size = self.size_formatted()
        return ('{}{} ({})'.format(icon, txt, size))

    def __repr__(self):
        return "<File (db={}, path={})>".format(self.h5file, self._path)

    def size(self):
        """
        return file size in kB

        Returns:
            int (kB)
        """
        args = {}
        result = self.conn.send_rcv(CMD_GET_FILESIZE, h5file=self.h5file,
                                    args=args)

        return result[RESPONSE_DATA]

    def size_formatted(self):
        """
        Return file size as a string. Example: "2.3G"
        """
        size = self.size()
        if size > 1000000000000:  # terabytes
            terabytes = size / 1000000000000
            return '{0:.1f}T'.format(terabytes)
        elif size > 1000000000:  # gigabytes
            gigabytes = size / 1000000000
            return '{0:.1f}G'.format(gigabytes)
        elif size > 1000000:  # megabytes
            megabytes = size / 1000000
            return '{0:.1f}M'.format(megabytes)
        elif size > 1000:  # kilobytes
            kilobytes = size / 1000
            return '{0:.0f}K'.format(kilobytes)
        else:  # bytes
            return '{0}b'.format(size)

    def rename(self, new):
        """
        Rename hdf5 file. Returns a new ``File`` object. Note that the
        existing object becomes unusable after renaming.

        Warning: existing ``Group`` and ``Dataset`` objects may contain
        references to the "old" file and become unusable after renaming the
        file.

        Args:
            new: new filename

        Returns:
            new ``File`` object

        Raises:
            TODO
        """
        result = self.conn.send_rcv(CMD_RENAME_DATABASE, h5file=self.h5file,
                                    args={CMD_KW_DB_RENAMETO: new})
        newFile = result[RESPONSE_DATA]
        self._h5file = None

        return newFile

    def delete(self):
        """
        Delete hdf5 file.
        """
        self.conn.send_rcv(CMD_DELETE_DATABASE, h5file=self.h5file, args={})
        self._h5file = None


class Dataset(Node):
    """
    Wrapper for h5py.Dataset
    """

    def __init__(self, conn, h5file, path, shape, dtype):
        Node.__init__(self, conn, h5file, path)
        self.__shape = shape
        self.__dtype = dtype

    def __repr__(self):
        return ("<Dataset {} {} (db={}, path={})>"
                .format(self.shape, self.dtype, self.h5file, self._path))

    def _repr_html_(self):
        """ representation in jupyter notebooks """
        txt = ("<strong>Dataset {} {} </strong> (file={}, path={})"
               .format(self.shape, self.dtype, self.h5file, self._path))
        icon = ICON_DATASET_ATTRS if len(self.attrs) > 0 else ICON_DATASET
        return ('<img style="{}" src="{}"/>{}'
                .format(IMG_STYLE, icon, txt))

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
        result = self.conn.send_rcv(CMD_SLICE_DATASET, h5file=self.h5file,
                                    args=args)
        return result[RESPONSE_DATA]

    def __setitem__(self, key, value):
        """
        Broadcasting for datasets. Example: mydataset[0,:] = np.arange(100)
        """
        args = {
            CMD_KW_PATH: self.path,
            CMD_KW_KEY: key,
        }
        self.conn.send_rcv(CMD_BROADCAST_DATASET, h5file=self.h5file,
                           args=args, data=value)

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

    def __init__(self, conn, h5file, path):
        """
        Args:
            conn: Connection object
            path: full path to hdf5 node
        """
        self.__conn = conn
        self.__h5file = h5file
        self.__path = path

    def __iter__(self):
        # In order to be compatible with h5py, we return a generator.
        for key in self.keys():
            yield key

    @property
    def conn(self):
        return self.__conn

    @conn.setter
    def conn(self, value):
        self.__conn = value

    @property
    def h5file(self):
        return self.__h5file

    def keys(self):
        """
        Returns attribute keys (list)
        """
        args = {
            CMD_KW_PATH: self.__path,
        }
        result = self.conn.send_rcv(CMD_ATTRIBUTES_KEYS, h5file=self.h5file,
                                    args=args)
        return result[RESPONSE_DATA][RESPONSE_ATTRS_KEYS]

    def __contains__(self, key):
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
        }
        result = self.conn.send_rcv(CMD_ATTRIBUTES_CONTAINS,
                                    h5file=self.h5file, args=args)
        return result[RESPONSE_DATA][RESPONSE_ATTRS_CONTAINS]

    def __len__(self):
        return len(self.keys())

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
        result = self.conn.send_rcv(CMD_ATTRIBUTES_GET, h5file=self.h5file,
                                    args=args)
        return result[RESPONSE_DATA]

    def __setitem__(self, key, value):
        """
        Set/overwrite attribute ``key`` with given ``value`` (scalar, string,
        or numpy array).
        """
        args = {
            CMD_KW_PATH: self.__path,
            CMD_KW_KEY: key,
        }

        self.conn.send_rcv(CMD_ATTRIBUTES_SET, h5file=self.h5file, args=args,
                           data=value)

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
        }

        try:
            response = self.conn.send_rcv(CMD_ATTRIBUTES_GET,
                                          h5file=self.h5file, args=args)
            result = response[RESPONSE_DATA]
        except NodeError as ne:
            if ne.status == KEY_ERROR:
                result = defaultvalue
            else:
                raise

        return result

    def to_dict(self):
        """
        Return attributes as dict
        """
        raise NotImplementedError()


class Tree(list):
    """
    list with nice tree representation in jupyter notebooks and ipython
    """

    def __init__(self, members):
        super().__init__(members)

    def __str__(self):
        """ text based tree representation """
        output = []

        def traverse(treenode, lastchild, depth, spaces):
            node, children = treenode
            if depth == 0:
                item = "──"
            elif lastchild:
                item = "└─"
            else:
                item = "├─"
            if isinstance(node, Group):
                txt = "/" if node.path == "/" else os.path.split(node.path)[1]
            else:  # dataset
                txt = node
            output.append("{}{} {}".format("".join(spaces), item, txt))
            spaces = spaces + ["    "] if lastchild else spaces + ["│   "]
            last = len(children) - 1
            for i, child in enumerate(children):
                traverse(child, i == last, depth + 1, spaces)

        lastchild = len(self[1]) > 0
        traverse(self, lastchild, 0, [])

        return "\n".join(output)

    def __repr__(self):
        return self.__str__()

    def _repr_html_(self):
        output = ['<ul class="hurraytree">\n']

        def traverse(treenode, depth=0):
            node, children = treenode
            if isinstance(node, Group):
                path = "/" if node.path == "/" else os.path.split(node.path)[1]
                img = ICON_GROUP_ATTRS if len(node.attrs) > 0 else ICON_GROUP
                output.append('<li>{}{}'.format(img, path))
            else:
                output.append('<li>{}'.format(node._repr_html_()))
            has_children = len(children) > 0
            if has_children:
                output.append("<ul>")
            for child in children:
                traverse(child, depth + 1)
            if has_children:
                output.append("</ul>")
            output.append("</li>")

        traverse(self)

        output.append("</ul>")
        html = "".join(output)
        css = '<style type="text/css">{}</style>'.format(CSS_TREE)

        return css + html
