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
Msgpack encoders and decoders for numpy "objects" (arrays, types,
scalars) and slices.
"""

from inspect import isclass

import numpy as np
from numpy.lib.format import header_data_from_array_1_0

from .nodes import File, Group, Dataset
from .protocol import (RESPONSE_H5FILE, RESPONSE_NODE_TYPE, NODE_TYPE_GROUP,
                       NODE_TYPE_FILE, NODE_TYPE_DATASET, RESPONSE_NODE_SHAPE,
                       RESPONSE_NODE_DTYPE, RESPONSE_NODE_PATH)


def encode(obj):
    """
    Encode numpy arrays and slices. Also converts numpy scalars and dtypes
    to pure Python objects.

    Args:
        obj: object to serialize

    Returns:
        dictionary or Python scalar
    """
    if isinstance(obj, np.ndarray):
        arr = header_data_from_array_1_0(obj)
        arr['arraydata'] = obj.tostring()
        arr['__ndarray__'] = True
        return arr
    elif isinstance(obj, slice):
        return {
            '__slice__': (obj.start, obj.stop, obj.step)
        }
    elif isclass(obj) and issubclass(obj, np.number):
        # make sure numpy type classes such as np.float64 (used, e.g., as dtype
        # arguments) are serialized to strings
        return obj().dtype.name
    elif isinstance(obj, np.dtype):
        return obj.name
    elif isinstance(obj, np.number):
        # convert to Python scalar
        return np.asscalar(obj)

    return obj


def get_decoder(connection):
    """
    Returns a msgpack decoder function, using ``connection`` to create proper
    ``Group`` and ``Dataset`` objects.

    Args:
        connection: Connection object that is assigned to decoded groups and
            datasets

    Returns:
        msgpack decoder function
    """

    def decode(obj):
        """
        msgpack decoder
        """

        if '__ndarray__' in obj:
            arr = np.fromstring(obj['arraydata'], dtype=np.dtype(obj['descr']))
            shape = obj[RESPONSE_NODE_SHAPE]
            arr.shape = shape
            if obj['fortran_order']:
                arr.shape = shape[::-1]
                arr = arr.transpose()
            return arr
        elif '__slice__' in obj:
            return slice(*obj['__slice__'])
        elif (isinstance(obj, dict)
              and obj.get(RESPONSE_NODE_TYPE, None) == NODE_TYPE_GROUP):
            # convert to Group object
            return Group(conn=connection, h5file=obj[RESPONSE_H5FILE],
                         path=obj[RESPONSE_NODE_PATH])
        elif (isinstance(obj, dict)
              and obj.get(RESPONSE_NODE_TYPE, None) == NODE_TYPE_FILE):
            # convert to File object
            return File(conn=connection, h5file=obj[RESPONSE_H5FILE],
                        path=obj[RESPONSE_NODE_PATH])
        elif (isinstance(obj, dict)
              and obj.get(RESPONSE_NODE_TYPE, None) == NODE_TYPE_DATASET):
            # convert to Dataset object
            return Dataset(conn=connection, h5file=obj[RESPONSE_H5FILE],
                           path=obj[RESPONSE_NODE_PATH],
                           shape=obj[RESPONSE_NODE_SHAPE],
                           dtype=obj[RESPONSE_NODE_DTYPE])

        return obj

    return decode
