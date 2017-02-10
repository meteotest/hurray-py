import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .msgpack_ext import MsgPackTestCase
    #from .nodes import NodeTestCase

    msgpack_suite = unittest.TestLoader().loadTestsFromTestCase(MsgPackTestCase)
    #node_suite = unittest.TestLoader().loadTestsFromTestCase(NodeTestCase)

    return unittest.TestSuite([msgpack_suite,])
