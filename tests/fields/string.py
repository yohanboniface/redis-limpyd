# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from datetime import datetime, timedelta

from limpyd import fields
from limpyd.exceptions import ImplementationError, UniquenessError

from ..model import TestRedisModel, BaseModelTest


class Vegetable(TestRedisModel):
    name = fields.StringField(indexable=True)
    color = fields.StringField()
    pip = fields.StringField(indexable=True)


class StringFieldTest(BaseModelTest):

    model = Vegetable

    def test_set_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.set('plum')

    def test_setnx_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.set('plum')
        # Try again now that is set
        with self.assertNumCommands(1):
            vegetable.color.set('plum')

    def test_setrange_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="dark green")
        with self.assertNumCommands(1):
            vegetable.color.setrange(5, 'blue')
        self.assertEqual(vegetable.color.get(), "dark bluen")

    def test_delete_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('plum')
        with self.assertNumCommands(1):
            vegetable.color.delete()

    def test_incr_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.incr()
        self.assertEqual(vegetable.color.get(), '1')
        with self.assertNumCommands(1):
            vegetable.color.incr()
        self.assertEqual(vegetable.color.get(), '2')

    def test_incrby_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.incrby(2)
        self.assertEqual(vegetable.color.get(), '2')
        with self.assertNumCommands(1):
            vegetable.color.incrby(3)
        self.assertEqual(vegetable.color.get(), '5')
        with self.assertNumCommands(1):
            vegetable.color.incrby(-2)
        self.assertEqual(vegetable.color.get(), '3')

    def test_incrbyfloat_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.incrbyfloat("1.5")
        self.assertEqual(vegetable.color.get(), '1.5')
        with self.assertNumCommands(1):
            vegetable.color.incrbyfloat(2.2)
        self.assertEqual(vegetable.color.get(), '3.7')
        with self.assertNumCommands(1):
            vegetable.color.incrbyfloat(-1.1)
        self.assertEqual(vegetable.color.get(), '2.6')

    def test_decr_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.decr()
        self.assertEqual(vegetable.color.get(), '-1')
        with self.assertNumCommands(1):
            vegetable.color.decr()
        self.assertEqual(vegetable.color.get(), '-2')

    def test_decrby_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.decrby(2)
        self.assertEqual(vegetable.color.get(), '-2')
        with self.assertNumCommands(1):
            vegetable.color.decrby(3)
        self.assertEqual(vegetable.color.get(), '-5')
        with self.assertNumCommands(1):
            vegetable.color.decrby(-2)
        self.assertEqual(vegetable.color.get(), '-3')

    def test_getset_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="green")
        with self.assertNumCommands(1):
            color = vegetable.color.getset("plum")
        self.assertEqual(color, "green")

    def test_append_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="dark")
        with self.assertNumCommands(1):
            vegetable.color.append(" green")
        self.assertEqual(vegetable.color.get(), "dark green")

    def test_setbit_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.setbit(0, 1)

    def test_delete_string(self):
        vegetable = self.model(name="aubergine")
        vegetable.name.delete()
        self.assertEqual(vegetable.name.get(), None)

    def test_setex_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.setex(3, 'green')
        self.assertEqual(vegetable.color.get(), "green")
        self.assertTrue(vegetable.color.ttl() > 0)
        vegetable.color.setex(2, 'dark green')
        self.assertEqual(vegetable.color.get(), "dark green")
        self.assertTrue(vegetable.color.ttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.ttl(), -1)

    def test_psetex_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.psetex(30, 'green')
        self.assertEqual(vegetable.color.get(), "green")
        self.assertTrue(vegetable.color.pttl() > 0)
        vegetable.color.psetex(20, 'dark green')
        self.assertEqual(vegetable.color.get(), "dark green")
        self.assertTrue(vegetable.color.pttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.pttl(), -1)

    def test_expire_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green')
        self.assertEqual(vegetable.color.ttl(), -1)
        vegetable.color.expire(2)
        self.assertTrue(vegetable.color.ttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.ttl(), -1)

    def test_pexpire_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green')
        self.assertEqual(vegetable.color.pttl(), -1)
        vegetable.color.pexpire(20)
        self.assertTrue(vegetable.color.pttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.pttl(), -1)

    def test_expireat_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green')
        self.assertEqual(vegetable.color.ttl(), -1)
        vegetable.color.expireat((datetime.now() + timedelta(seconds=2)).replace(microsecond=0))
        self.assertTrue(vegetable.color.ttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.ttl(), -1)

    def test_pexpireat_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green')
        self.assertEqual(vegetable.color.pttl(), -1)
        vegetable.color.pexpireat((datetime.now() + timedelta(seconds=2)))
        self.assertTrue(vegetable.color.pttl() > 0)
        vegetable.color.persist()
        self.assertEqual(vegetable.color.pttl(), -1)

    def test_set_with_ex_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green', ex=3)
        self.assertTrue(vegetable.color.ttl() > 0)

    def test_set_with_px_is_possible_if_not_indexable(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('green', px=3000)
        self.assertTrue(vegetable.color.pttl() > 0)


class IndexableStringFieldTest(BaseModelTest):

    model = Vegetable

    def test_set_should_be_indexed(self):
        vegetable = self.model()
        vegetable.name.set('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')

        vegetable.name.set('pepper')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='pepper')

    def test_getset_should_deindex_before_reindexing(self):
        vegetable = self.model()
        name = vegetable.name.getset('aubergine')
        self.assertIsNone(name)
        self.assertCollection([vegetable._pk], name='aubergine')

        name = vegetable.name.getset('pepper')
        self.assertEqual(name, 'aubergine')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='pepper')

    def test_delete_should_deindex(self):
        vegetable = self.model()
        vegetable.name.set('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        vegetable.name.delete()
        self.assertCollection([], name='aubergine')

    def test_append_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.name.set('sweet')
        self.assertCollection([vegetable._pk], name='sweet')
        vegetable.name.append(' pepper')
        self.assertCollection([], name='sweet')
        self.assertCollection([vegetable._pk], name='sweet pepper')

    def test_decr_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            # + n for the lock
            vegetable.pip.decr()
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=9)

    def test_decrby_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            # + n for the lock
            vegetable.pip.decrby(3)
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=7)

    def test_incr_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            # + n for the lock
            vegetable.pip.incr()
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=11)

    def test_incrby_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            # + n for the lock
            vegetable.pip.incr(3)
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=13)

    def test_incrbyfloat_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set("10.3")
        self.assertCollection([vegetable._pk], pip="10.3")
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            # + n for the lock
            vegetable.pip.incrbyfloat("3.9")
        self.assertCollection([], pip="10.3")
        self.assertCollection([vegetable._pk], pip="14.2")

    def test_setnx_should_index_only_if_value_has_been_set(self):
        vegetable = self.model()
        vegetable.name.setnx('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        with self.assertNumCommands(1 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 1 for setnx
            # + n for the lock
            vegetable.name.setnx('pepper')
        self.assertCollection([], name='pepper')

    def test_setrange_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.name.setnx('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        with self.assertNumCommands(5 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for deindex (getting value from redis)
            # - 1 for setrange
            # - 2 for reindex (getting value from redis)
            # + n for the lock
            vegetable.name.setrange(2, 'gerb')
        self.assertEqual(vegetable.name.get(), 'augerbine')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='augerbine')

    def test_setbit_should_deindex_and_reindex(self):
        vegetable = self.model(name="aubergine", pip='@')  # @ = 0b01000000
        with self.assertNumCommands(5 + self.COUNT_LOCK_COMMANDS):
            # Check number of queries
            # - 2 for deindex (getting value from redis)
            # - 1 for setbit
            # - 2 for reindex (getting value from redis)
            # + n for the lock
            vegetable.pip.setbit(3, 1)  # 01010000 => P
        self.assertEqual(vegetable.pip.get(), 'P')
        self.assertCollection([], pip='@')
        self.assertCollection([vegetable._pk], pip='P')

    def test_setex_is_not_possible_if_indexable(self):
        vegetable = self.model()
        with self.assertRaises(ImplementationError):
            vegetable.name.setex(3, 'aubergine')

    def test_psetex_is_not_possible_if_indexable(self):
        vegetable = self.model()
        with self.assertRaises(ImplementationError):
            vegetable.name.psetex(30, 'aubergine')

    def test_expire_is_not_possible_if_indexable(self):
        vegetable = self.model(name='aubergine')
        with self.assertRaises(ImplementationError):
            vegetable.name.expire(3)

    def test_pexpire_is_not_possible_if_indexable(self):
        vegetable = self.model(name='aubergine')
        with self.assertRaises(ImplementationError):
            vegetable.name.pexpire(30)

    def test_expireat_is_not_possible_if_indexable(self):
        vegetable = self.model(name='aubergine')
        with self.assertRaises(ImplementationError):
            vegetable.name.expireat((datetime.now() + timedelta(seconds=2)).replace(microsecond=0))

    def test_pexpireat_is_not_possible_if_indexable(self):
        vegetable = self.model(name='aubergine')
        with self.assertRaises(ImplementationError):
            vegetable.name.pexpireat((datetime.now() + timedelta(seconds=2)))

    def test_set_with_ex_is_not_possible_if_indexable(self):
        vegetable = self.model()
        with self.assertRaises(ImplementationError):
            vegetable.name.set('aubergine', ex=3)

    def test_set_with_px_is_not_possible_if_indexable(self):
        vegetable = self.model()
        with self.assertRaises(ImplementationError):
            vegetable.name.set('aubergine', px=30)


class Ferry(TestRedisModel):
    name = fields.StringField(unique=True)


class UniqueStringFieldTest(BaseModelTest):

    model = Ferry

    def test_unique_stringfield_should_not_be_settable_twice_at_init(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        with self.assertRaises(UniquenessError):
            self.model(name=u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")

    def test_set_should_hit_uniqueness_check(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Danièle Casanova")
        with self.assertRaises(UniquenessError):
            ferry2.name.set(u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Danièle Casanova")

    def test_getset_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Danièle Casanova")
        with self.assertRaises(UniquenessError):
            ferry2.name.getset(u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Danièle Casanova")

    def test_append_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Napoléon")
        with self.assertRaises(UniquenessError):
            ferry2.name.append(u" Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Napoléon")

    def test_decr_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=1)
        ferry2 = self.model(name=2)
        with self.assertRaises(UniquenessError):
            ferry2.name.decr()
        self.assertCollection([ferry1._pk], name=1)
        self.assertCollection([ferry2._pk], name=2)

    def test_incr_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=2)
        ferry2 = self.model(name=1)
        with self.assertRaises(UniquenessError):
            ferry2.name.incr()
        self.assertCollection([ferry1._pk], name=2)
        self.assertCollection([ferry2._pk], name=1)

    def test_incrbyfloat_should_hit_uniqueness_test(self):
        ferry1 = self.model(name="3.0")
        ferry2 = self.model(name="2.5")
        with self.assertRaises(UniquenessError):
            ferry2.name.incrbyfloat("0.5")
        self.assertCollection([ferry1._pk], name="3.0")
        self.assertCollection([ferry2._pk], name="2.5")

    def test_setrange_should_hit_uniqueness_test(self):
        ferry1 = self.model(name="Kalliste")
        ferry2 = self.model(name="Kammiste")
        with self.assertRaises(UniquenessError):
            ferry2.name.setrange(2, "ll")
        self.assertCollection([ferry1._pk], name="Kalliste")
        self.assertCollection([ferry2._pk], name="Kammiste")

    def test_setbit_should_hit_uniqueness_test(self):
        ferry1 = self.model(name='P')  # 0b01010000
        ferry2 = self.model(name='@')  # 0b01000000
        with self.assertRaises(UniquenessError):
            ferry2.name.setbit(3, 1)
        self.assertCollection([ferry1._pk], name='P')
        self.assertCollection([ferry2._pk], name='@')
