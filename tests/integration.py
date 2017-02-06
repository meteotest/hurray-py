import random
import string
import unittest
import os

import numpy as np
from numpy.testing import assert_array_equal

import hurraypy as hp


def random_name(l):
    return ''.join(random.SystemRandom().choice(string.ascii_lowercase)
                   for _ in range(l))


class IntegrationTest(unittest.TestCase):
    """
    Make sure you have a hurray server running at localhost:2222 or a socket at
    /tmp/hurray.socket You probably want to clean the database files after
    testing
    """

    def setUp(self):
        self.conn = hp.connect('localhost', '2222')
        # self.uds_conn = hp.connect(unix_socket_path='/tmp/hurray.socket')

        # create a file
        db_name = 'htest-' + random_name(5) + '.h5'
        self.conn.create_file(db_name)
        self.db = self.conn.use_file(db_name)

    def tearDown(self):
        pass
        # TODO delete self.db

    def test_group_operations(self):
        db = self.db
        # create and get a group
        db.create_group("/mygrp")
        db.require_group("/mygrp/subgrp/subsubgrp")
        db.require_group("/mygrp/subgrp2")
        grp = db["/mygrp"]

        self.assertEqual(grp.path, "/mygrp")
        self.assertEqual(db.keys(), ("mygrp",))
        self.assertEqual(db["/"].keys(), ("mygrp",))
        self.assertEqual(db["/mygrp"].keys(),
                         ("subgrp", "subgrp2"))

    def test_dataset_operations(self):
        db = self.db

        array_name = 'myarray'
        array_path = os.path.join("/mygrp3", array_name)
        grp = db.create_group("/mygrp3")

        data = np.array([[1, 2, 3], [4, 5, 6]])
        ds = grp.create_dataset(array_name, data=data)
        self.assertEqual(ds.path, array_path)

        dataset = db[array_path]

        assert_array_equal(data, dataset[:])
        assert_array_equal(data[1], dataset[1])

        x = np.array([8, 9, 10])
        dataset[0, :] = x
        assert_array_equal(np.array([x, data[1]]), dataset[:])

        # test require_dataset()
        import ipdb; ipdb.set_trace()
        dst = grp.require_dataset("myarray2", data=data)
        dst = grp.require_dataset("myarray2", data=data)
        self.assertEqual(ds.path, array_path)
        assert_array_equal(data, dst[:])
        assert_array_equal(data[1], dst[1])

    def test_attr_operations(self):
        db = self.db

        data = np.array([[1, 2, 3], [4, 5, 6]])
        dataset = db.create_dataset("dataset_foo", data=data)

        attr_key = 'foo'
        attr_value = 'bar'

        dataset.attrs[attr_key] = attr_value
        self.assertEqual(dataset.attrs['foo'], attr_value)

        self.assertTrue(attr_key in dataset.attrs)
        self.assertFalse('no' in dataset.attrs)

        self.assertEqual(dataset.attrs.keys(), (attr_key,))

        attr_value_array = np.array([0.1, 0.2, 0.5])

        dataset.attrs['num'] = attr_value_array
        assert_array_equal(dataset.attrs['num'], attr_value_array)

    def test_special_operations(self):
        db = self.db

        db.require_group("/mygrp/subgrp/subsubgrp")
        db.require_group("/mygrp/subgrp2")
        db.create_dataset("/mygrp/mydataset",
                          data=np.random.random((200, 300)))

        def itemvisitor(name, node):
            print(name, node)

        db.visititems(itemvisitor)

        def visitor(name):
            print(name)

        db.visit(visitor)
