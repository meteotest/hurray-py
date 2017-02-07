"""
Compare hurray performance with h5py performance.

Make sure to install h5py to run these tests.
"""

import random
import string
import os
import sys
import timeit

import numpy as np
import h5py

if __name__ == "__main__":
    here = os.path.dirname(os.path.realpath(__file__))
    path = os.path.abspath(os.path.join(here, '../'))
    sys.path.insert(0, path)

import hurraypy as hrr


def random_name(l):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase)
                   for _ in range(l))


def main():
    repetitions = 3
    for testcase in ("bigdata", "smalldata"):
        secs_hurray = timeit.timeit("test('hurray', '{}')".format(testcase),
                                    number=repetitions,
                                    setup="from __main__ import test")
        secs_hurray_uds = timeit.timeit("test('hurray_uds', '{}')"
                                        .format(testcase),
                                        number=repetitions,
                                        setup="from __main__ import test")
        secs_h5py = timeit.timeit("test('h5py', '{}')".format(testcase),
                                  number=repetitions,
                                  setup="from __main__ import test")
        print("\ntest case '{}'".format(testcase))
        print("#####################\n")
        print("Total running time for {} repetitions:".format(repetitions))
        print("hurray TCP:\t{:.2f} seconds".format(secs_hurray))
        print("hurray UDS:\t{:.2f} seconds".format(secs_hurray_uds))
        print("plain h5py:\t{:.2f} seconds".format(secs_h5py))
        print("=> h5py was ~{:.0f} times faster than hurray over TCP"
              .format(secs_hurray / secs_h5py))
        print("=> h5py was ~{:.0f} times faster than hurray over UDS"
              .format(secs_hurray_uds / secs_h5py))


def test(engine, testcase="bigdata"):
    """
    Args:
        engine: either "hurray", "hurray_uds" (hurray over unix domain
            sockets) or "h5py"
        testcase: test with either "bigdata" or "smalldata"
    """
    if testcase == "bigdata":
        iterations = 2
        data_shape = (500, 1500)
    else:
        iterations = 20
        data_shape = (4, 7)

    filename = 'htest-' + random_name(5) + '.h5'

    if engine == "hurray":
        conn = hrr.connect('localhost', '2222')
        file_ = conn.create_file(filename)
    elif engine == "hurray_uds":
        conn = hrr.connect(unix_socket_path='/tmp/hurray.socket')
        file_ = conn.create_file(filename)
    elif engine == "h5py":
        filename = os.path.join("/tmp/", filename)
        file_ = h5py.File(filename)
    else:
        raise ValueError("unknown engine")

    for i in range(iterations):
        data = np.random.random(data_shape)

        file_.create_group("/mygrp{}".format(i))
        file_.require_group("/mygrp{}/subgrp/subsubgrp".format(i))
        array_path = "/datagrp/myarray{}".format(i)
        dst = file_.create_dataset(array_path, data=data)
        dst.attrs["unit"] = "meters"

        data = dst[:]

    if engine == "h5py":
        os.unlink(filename)


if __name__ == "__main__":
    main()
