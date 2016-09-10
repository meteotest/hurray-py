import unittest

import msgpack
import numpy as np
from numpy.testing import assert_array_equal

from hurraypy.msgpack_numpy import encode_np_array, decode_np_array


class MsgPackTest(unittest.TestCase):
    def test_ndarray(self):
        data_in = np.random.randint(0, 255, dtype='u1', size=(5, 10))

        packed_nparray = msgpack.packb(data_in, default=encode_np_array)
        unpacked_nparray = msgpack.unpackb(packed_nparray, object_hook=decode_np_array)

        assert_array_equal(data_in, unpacked_nparray)

    def test_slice(self):
        slice_in = slice(0, 1, 0)

        packed_slice = msgpack.packb(slice_in, default=encode_np_array)
        unpacked_slice = msgpack.unpackb(packed_slice, object_hook=decode_np_array)

        self.assertEqual(slice_in, unpacked_slice)
