import unittest
from unittest.mock import patch

import numpy as np
from numpy.testing import assert_array_equal

import hurraypy as hp
from hurraypy.exceptions import DatabaseError, MessageError, NodeError
from hurraypy.nodes import Group, Dataset
from hurraypy.status_codes import (INVALID_ARGUMENT, FILE_EXISTS,
                                   FILE_NOT_FOUND, GROUP_EXISTS,
                                   DATASET_EXISTS, NODE_NOT_FOUND, VALUE_ERROR,
                                   TYPE_ERROR, KEY_ERROR)
from tests.server_mock import MockServer


def _connect(*args):
    """
    TCP or Unix domain socket mock. Returns nothing instead of a socket reader
    and writer as the send_rcv call is mocked
    """
    return None, None


class NodeTestCase(unittest.TestCase):

    def setUp(self):
        self.ms = MockServer()

        self.run = patch('hurraypy.client.Connection._run',
                         side_effect=self.ms.run)
        self.connect = patch('hurraypy.client.Connection._connect',
                             side_effect=_connect)
        self.run_mock = self.run.start()
        self.connect_mock = self.connect.start()

        self.conn = hp.connect('host', 'port')

    def tearDown(self):
        self.run_mock.stop()
        self.connect_mock.stop()

    def test_create_connect(self):
        db_name = 'test.h5'
        self.conn.create_file(db_name)
        self.run_mock.assert_called_once_with('create_db', {'db': db_name},
                                              None)

        with self.assertRaises(DatabaseError) as context:
            self.conn.create_file(db_name)

        self.assertEqual(context.exception.status, FILE_EXISTS)

        db = self.conn.use_file(db_name)

        self.assertIsInstance(db, Group)
        self.assertEqual(db._conn.db, db_name)
        self.assertEqual(db._path, '/')

        with self.assertRaises(DatabaseError) as context:
            self.conn.use_file('invalid')

        self.assertEqual(context.exception.status, FILE_NOT_FOUND)

    def test_group(self):
        db_name = 'test.h5'
        self.conn.create_file(db_name)
        db = self.conn.use_file(db_name)

        group_path = '/mygrp'

        # Create group
        db.create_group(group_path)

        self.run_mock.assert_called_with('create_group',
                                         {'db': db_name, 'path': group_path},
                                         None)

        # Invalid group path
        with self.assertRaises(NodeError) as context:
            db['invalid']

        self.assertEqual(context.exception.status, NODE_NOT_FOUND)

        # Get group
        grp = db[group_path]

        self.run_mock.assert_called_with('get_node',
                                         {'db': db_name, 'path': group_path},
                                         None)

        self.assertIsInstance(grp, Group)
        self.assertEqual(grp._conn.db, db_name)
        self.assertEqual(grp._path, group_path)

        # Create relative sub group
        sub_path = 'sub'
        grp.create_group(sub_path)
        self.run_mock.assert_called_with('create_group',
                                         {'db': db_name, 'path': group_path
                                          + '/' + sub_path},
                                         None)

        # Try to create group at same path
        with self.assertRaises(NodeError) as context:
            grp.create_group(sub_path)

        self.assertEqual(context.exception.status, GROUP_EXISTS)

        attr_key = 'key'
        attr_value = 'value'

        # Invalid attribute key
        with self.assertRaises(MessageError) as context:
            grp.attrs[''] = 'value'

        self.assertEqual(context.exception.status, INVALID_ARGUMENT)

        # Set attribute
        grp.attrs[attr_key] = 'value'
        self.run_mock.assert_called_with('attrs_setitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': group_path},
                                         attr_value)

        # Get attribute
        val = grp.attrs[attr_key]
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': group_path},
                                         None)
        self.assertEqual(val, attr_value)

        # Get non-existing attribute
        with self.assertRaises(NodeError) as context:
            grp.attrs['invalid']

        self.assertEqual(context.exception.status, KEY_ERROR)

        # Get attribute with default val
        val = grp.attrs.get(attr_key, None)
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': group_path}, None)
        self.assertEqual(val, attr_value)

        # Get attribute with default val
        default = 'default'
        val = grp.attrs.get('invalid', default)
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': 'invalid', 'db': db_name,
                                          'path': group_path}, None)
        self.assertEqual(val, default)

    def test_dataset(self):
        db_name = 'test.h5'
        self.conn.create_file(db_name)
        db = self.conn.use_file(db_name)

        data_path = '/mydata'
        data = np.array([[1, 2, 3], [4, 5, 6]])

        # Create group
        db.create_dataset(data_path, data=data)

        self.run_mock.assert_called_with('create_dataset',
                                         {'db': db_name, 'path': data_path},
                                         data)

        # Invalid dataset path
        with self.assertRaises(NodeError) as context:
            db['invalid']

        self.assertEqual(context.exception.status, NODE_NOT_FOUND)

        # Get data
        dataset = db[data_path]
        self.assertIsInstance(dataset, Dataset)
        self.assertEqual(dataset._conn.db, db_name)
        self.assertEqual(dataset._path, data_path)
        self.run_mock.assert_called_with('get_node',
                                         {'db': db_name, 'path': data_path},
                                         None)

        # Slice data
        assert_array_equal(dataset[:], data)
        self.run_mock.assert_called_with('slice_dataset',
                                         {'db': db_name, 'path': data_path,
                                          'key': slice(None, None, None)},
                                         None)

        # Try to create dataset at same path
        with self.assertRaises(NodeError) as context:
            db.create_dataset(data_path, data=data)

        self.assertEqual(context.exception.status, DATASET_EXISTS)

        # Broadcast data
        x = np.array([8, 9, 10])
        dataset[0, :] = x

        self.run_mock.assert_called_with('broadcast_dataset',
                                         {'db': db_name, 'path': data_path,
                                          'key': (0, slice(None, None, None))},
                                         x)
        assert_array_equal(np.array([x, data[1]]), dataset[:])

        # Wrong data
        with self.assertRaises(NodeError) as context:
            dataset[0, :] = np.array([8, 9, 10, 11])

        self.assertEqual(context.exception.status, VALUE_ERROR)

        # Wrong slice
        with self.assertRaises(NodeError) as context:
            dataset[3, :] = np.array([8, 9, 10])

        self.assertEqual(context.exception.status, TYPE_ERROR)

        attr_key = 'key'
        attr_value = 'value'

        # Invalid attribute key
        with self.assertRaises(MessageError) as context:
            dataset.attrs[''] = 'value'

        self.assertEqual(context.exception.status, INVALID_ARGUMENT)

        # Set attribute
        dataset.attrs[attr_key] = 'value'
        self.run_mock.assert_called_with('attrs_setitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': data_path},
                                         attr_value)

        # Get attribute
        val = dataset.attrs[attr_key]
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': data_path},
                                         None)
        self.assertEqual(val, attr_value)

        # Get non-existing attribute
        with self.assertRaises(NodeError) as context:
            dataset.attrs['invalid']

        self.assertEqual(context.exception.status, KEY_ERROR)

        # Get attribute with default val
        val = dataset.attrs.get(attr_key, None)
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': attr_key, 'db': db_name,
                                          'path': data_path}, None)
        self.assertEqual(val, attr_value)

        # Get attribute with default val
        default = 'default'
        val = dataset.attrs.get('invalid', default)
        self.run_mock.assert_called_with('attrs_getitem',
                                         {'key': 'invalid', 'db': db_name,
                                          'path': data_path},
                                         None)
        self.assertEqual(val, default)
