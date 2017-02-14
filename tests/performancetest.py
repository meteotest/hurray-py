"""
Compare hurray performance with h5py performance.

Make sure to install h5py and h5pySWMR to run these tests.
"""
import os
import random
import string
import sys
import timeit

import h5py
import h5pyswmr
import numpy as np
from numpy.testing import assert_array_equal

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
    engines = ("hurray", "hurray_uds", "h5py", "h5pyswmr")
    results = {"bigdata": {}, "smalldata": {}}
    for test_case in ("bigdata", "smalldata"):
        for engine in engines:
            conn = None
            if engine == "hurray":
                conn = hrr.connect("localhost", "2222")
            elif engine == "hurray_uds":
                conn = hrr.connect(unix_socket_path="/tmp/hurray.socket")

            def timer_function(function, *args):
                def wrap():
                    function(*args)

                t = timeit.Timer(wrap)
                return t.timeit(repetitions)

            results[test_case][engine] = timer_function(test, engine, test_case, conn)

    for test_case, result in results.items():
        print("\ntest case '{}'".format(test_case))
        print("#####################\n")
        print("Total running time for {} repetitions:".format(repetitions))
        for engine in engines:
            sec = results[test_case][engine]
            print("{}:{}{:.2f}s".format(engine, " " * (11 - len(engine)), sec))
        secs_hurray = results[test_case]["hurray"]
        secs_hurray_uds = results[test_case]["hurray_uds"]
        print("⇒ h5py was ~{:.0f} times faster than hurray over TCP"
              .format(secs_hurray / results[test_case]["h5py"]))
        print("⇒ h5py was ~{:.0f} times faster than hurray over UDS"
              .format(secs_hurray_uds / results[test_case]["h5py"]))
        print("⇒ h5pyswmr was ~{:.0f} times faster than hurray over TCP"
              .format(secs_hurray / results[test_case]["h5pyswmr"]))
        print("⇒ h5pyswmr was ~{:.0f} times faster than hurray over UDS"
              .format(secs_hurray_uds / results[test_case]["h5pyswmr"]))


def test(engine, testcase="bigdata", connection=None):
    """
    Args:
        engine: either "hurray", "hurray_uds" (hurray over unix domain
            sockets), "h5py", or "h5pyswmr"
        testcase: test with either "bigdata" or "smalldata"
        connection: hurray server connection
    """
    if testcase == "bigdata":
        iterations = 2
        data_shape = (500, 1500)
    else:
        iterations = 20
        data_shape = (4, 7)

    filename = "htest-" + random_name(5) + ".h5"

    if engine == "hurray":
        file_ = connection.create_file(filename)
    elif engine == "hurray_uds":
        file_ = connection.create_file(filename)
    elif engine == "h5py":
        filename = os.path.join("/tmp/", filename)
        file_ = h5py.File(filename)
    elif engine == "h5pyswmr":
        filename = os.path.join("/tmp/", filename)
        file_ = h5pyswmr.File(filename)
    else:
        raise ValueError("unknown engine")

    for i in range(iterations):
        data = np.random.random(data_shape)

        file_.create_group("/mygrp{}".format(i))
        file_.require_group("/mygrp{}/subgrp/subsubgrp".format(i))
        array_path = "/datagrp/myarray{}".format(i)
        dst = file_.create_dataset(name=array_path, data=data)
        dst.attrs["unit"] = "meters"
        assert_array_equal(dst[:], data)

    if engine == "h5py" or engine == "h5pyswmr":
        os.unlink(filename)


if __name__ == "__main__":
    main()
