# -*- coding:utf-8 -*-

import unittest
import logging
from StringIO import StringIO

from limpyd.middlewares import BaseMiddleware, LoggingMiddleware
from limpyd.database import RedisDatabase
from limpyd import model
from limpyd import fields

from base import LimpydBaseTest, TEST_CONNECTION_SETTINGS


class ForceSetterMiddleware(BaseMiddleware):
    """
    A test middleware that always save the same given value for all "set" calls
    """
    def __init__(self, value):
        super(ForceSetterMiddleware, self).__init__()
        self.value = value

    def pre_command(self, command, context):
        if command.name == 'hset':
            command.kwargs = {}
            command.args = (command.args[0], command.args[1], self.value)


class ForceGetterMiddleware(BaseMiddleware):
    """
    A test middleware that always returns the same given value for all "get" calls
    """
    def __init__(self, value):
        super(ForceGetterMiddleware, self).__init__()
        self.value = value

    def post_command(self, command, result, context):
        if command.name == 'hget':
            result.value = self.value
        return result


class BaseTestModel(model.RedisModel):
    abstract = True
    cacheable = False
    foo = fields.InstanceHashField()


class MiddlewareTest(LimpydBaseTest):
    def test_middleware_pre_command_method_should_be_called(self):
        test_database = RedisDatabase(middlewares=[
            ForceSetterMiddleware(value='BAZ'),
        ], **TEST_CONNECTION_SETTINGS)

        class TestModel(BaseTestModel):
            database = test_database
            namespace = 'test_middleware_pre_command_method_should_be_called'

        instance = TestModel(foo='bar')
        self.assertEqual(instance.foo.hget(), 'BAZ')

    def test_middleware_post_command_method_should_be_called(self):
        test_database = RedisDatabase(middlewares=[
            ForceGetterMiddleware(value='QUX'),
        ], **TEST_CONNECTION_SETTINGS)

        class TestModel(BaseTestModel):
            database = test_database
            namespace = 'test_middleware_post_command_method_should_be_called'

        instance = TestModel(foo='bar')

        # the middleware will send "QUX"
        self.assertEqual(instance.foo.hget(), 'QUX')

        # but for untouched command, we got the real values
        self.assertEqual(instance.hmget('foo'), ['bar'])

    def test_database_can_accept_many_middlewares(self):
        test_database = RedisDatabase(middlewares=[
            ForceSetterMiddleware(value='BAZ'),
            ForceGetterMiddleware(value='QUX'),
        ], **TEST_CONNECTION_SETTINGS)

        class TestModel(BaseTestModel):
            database = test_database
            namespace = 'test_database_can_accept_many_middlewares'

        instance = TestModel(foo='bar')

        # the getter middleware will send "QUX"
        self.assertEqual(instance.foo.hget(), 'QUX')

        # but for untouched command, we got the value set by the setter middleware
        self.assertEqual(instance.hmget('foo'), ['BAZ'])

    def test_logging_middleware(self):

        logger = logging.getLogger('limpyd.tests.middlewares.test_logging_middleware')
        stream = StringIO()
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(stream))

        test_database = RedisDatabase(middlewares=[
            LoggingMiddleware(logger, log_time=False)
        ], **TEST_CONNECTION_SETTINGS)

        class TestModel(BaseTestModel):
            database = test_database
            namespace = 'test_logging_middleware'

        instance = TestModel(foo='bar')
        self.assertEqual(instance.foo.hget(), 'bar')

        log_lines = [line for line in stream.getvalue().split('\n') if line]
        self.assertEqual(len(log_lines), 4)
        self.assertEqual(log_lines[0], u"[#1] Command(name='hset', args=(u'test_logging_middleware:testmodel:1:hash', 'foo', 'bar'), kwargs={})")
        self.assertEqual(log_lines[1], u"[#1] Result(value=1)")
        self.assertEqual(log_lines[2], u"[#2] Command(name='hget', args=(u'test_logging_middleware:testmodel:1:hash', 'foo'), kwargs={})")
        self.assertEqual(log_lines[3], u"[#2] Result(value=bar)")

    def test_logging_middleware_with_another(self):

        logger = logging.getLogger('limpyd.tests.middlewares.test_logging_middleware_with_another')
        stream = StringIO()
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(stream))

        test_database = RedisDatabase(middlewares=[
            ForceSetterMiddleware(value='BAZ'),
            LoggingMiddleware(logger, log_time=False)
        ], **TEST_CONNECTION_SETTINGS)

        class TestModel(BaseTestModel):
            database = test_database
            namespace = 'test_logging_middleware_with_another'

        instance = TestModel(foo='bar')
        self.assertEqual(instance.foo.hget(), 'BAZ')

        log_lines = [line for line in stream.getvalue().split('\n') if line]
        self.assertEqual(len(log_lines), 4)
        self.assertEqual(log_lines[0], u"[#1] Command(name='hset', args=(u'test_logging_middleware_with_another:testmodel:1:hash', 'foo', 'BAZ'), kwargs={})")
        self.assertEqual(log_lines[1], u"[#1] Result(value=1)")
        self.assertEqual(log_lines[2], u"[#2] Command(name='hget', args=(u'test_logging_middleware_with_another:testmodel:1:hash', 'foo'), kwargs={})")
        self.assertEqual(log_lines[3], u"[#2] Result(value=BAZ)")

if __name__ == '__main__':
    unittest.main()
