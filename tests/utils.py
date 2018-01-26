# -*- coding:utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from future.builtins import str

import unittest

from limpyd.utils import make_key, unique_key

from .base import LimpydBaseTest


class MakeKeyTest(LimpydBaseTest):

    def test_simple_key(self):
        self.assertEqual("simple_key", make_key("simple_key"))

    def test_multi_element_key(self):
        self.assertEqual("complex:key", make_key("complex", "key"))

    def test_unicode_element(self):
        self.assertEqual(u"french:key:clé", make_key("french", "key", u"clé"))

    def test_integer_element(self):
        self.assertEqual("integer:key:1", make_key("integer", "key", 1))


class UniqueKeyTest(LimpydBaseTest):

    def test_generated_key_must_be_a_string(self):
        key = unique_key(self.connection)
        self.assertTrue(isinstance(key, str))

    def test_generated_key_must_be_unique(self):
        key1 = unique_key(self.connection)
        key2 = unique_key(self.connection)
        self.assertNotEqual(key1, key2)


class LimpydBaseTestTest(LimpydBaseTest):
    """
    Test parts of LimpydBaseTest
    """

    def test_assert_num_commands_is_ok(self):
        with self.assertNumCommands(1):
            # we know that info do only one command
            self.connection.info()

    def test_assert_count_key_is_ok(self):
        self.assertEqual(self.count_keys(), 0)
        self.connection.set('__test__', '__test__')
        self.assertEqual(self.count_keys(), 1)


if __name__ == '__main__':
    unittest.main()
