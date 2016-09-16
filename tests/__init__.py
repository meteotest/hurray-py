import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .msgpack_ext import MsgPackTestCase

    msgpack_suite = unittest.TestLoader().loadTestsFromTestCase(MsgPackTestCase)

    return unittest.TestSuite([msgpack_suite])
