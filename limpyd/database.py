# -*- coding:utf-8 -*-

import redis

from limpyd.exceptions import *

from logging import getLogger
log = getLogger(__name__)


DEFAULT_CONNECTION_SETTINGS = dict(
    host="localhost",
    port=6379,
    db=0
)


class Command(object):
    """
    Object to pass the command through middlewares
    """
    __slots__ = ('name', 'args', 'kwargs',)

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def __unicode__(self):
        return u"Command(name='%s', args=%s, kwargs=%s)" % (self.name, self.args, self.kwargs)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        return str(self)


class Result(object):
    """
    Object to pass the command's result through middlewares
    """
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

    def __unicode__(self):
        return u"Result(value=%s)" % self.value

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __repr__(self):
        return str(self)


class RedisDatabase(object):
    """
    A RedisDatabase regroups some models and handles the connection to Redis for
    them.
    Each model must have a database entry, but many (or all) can share the same
    RedisDatabase object (so each of these models will be stored on the same
    Redis server+database)
    In a database, two models with the same namespace (empty by default) cannot
    have the same name (defined by the class name)
    """
    _connections = {}  # class level cache
    discard_cache = False
    middlewares = []

    def __init__(self, middlewares=None, **connection_settings):
        self._connection = None  # Instance level cache
        self.reset(**(connection_settings or DEFAULT_CONNECTION_SETTINGS))

        # _models keep an entry for each defined model on this database
        self._models = dict()

        if middlewares is not None:
            self.middlewares = middlewares

        super(RedisDatabase, self).__init__()

    def connect(self, **settings):
        """
        Connect to redis and cache the new connection
        """
        # compute a unique key for this settings, for caching. Work on the whole
        # dict without directly using known keys to allow the use of unix socket
        # connection or any other (future ?) way to connect to redis
        if not settings:
            settings = self.connection_settings
        connection_key = ':'.join([str(settings[k]) for k in sorted(settings)])
        if connection_key not in self._connections:
            self._connections[connection_key] = redis.StrictRedis(**settings)
        return self._connections[connection_key]

    def reset(self, **connection_settings):
        """
        Set the new connection settings to be used and reset the connection
        cache so the next redis call will use these settings.
        """
        self.connection_settings = connection_settings
        self._connection = None

    def _add_model(self, model):
        """
        Save this model as one existing on this database, to deny many models
        with same namespace and name
        """
        if model._name in self._models:
            raise ImplementationError(
                'A model with namespace "%s" and name "%s" is already defined '
                'on this database' % (model.namespace, model.__name__))
        self._models[model._name] = model

    def _use_for_model(self, model):
        """
        Update the given model to use the current database. Do it also for all
        of its subclasses if they share the same database. (so it's easy to
        call use_database on an abstract model to use the new database for all
        subclasses)
        """
        original_database = getattr(model, 'database', None)

        def get_models(model):
            """
            Return the model and all its submodels that are on the same database
            """
            model_database = getattr(model, 'database', None)
            if model_database == self:
                return []
            models = [model]
            for submodel in model.__subclasses__():
                if getattr(submodel, 'database', None) == model_database:
                    models += get_models(submodel)
            return models

        # put the model and all its matching submodels on the new database
        models = get_models(model)
        for _model in models:
            if not _model.abstract:
                self._add_model(_model)
                del original_database._models[_model._name]
            _model.database = self

        # return updated models
        return models

    @property
    def connection(self):
        """
        A simple property on the instance that return the connection stored on
        the class
        """
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    def has_scripting(self):
        """
        Returns True if scripting is available. Checks are done in the client
        library (redis-py) AND the redis server. Resut is cached, so done only
        one time.
        """
        if not hasattr(self, '_has_scripting'):
            try:
                version = float('%s.%s' %
                    tuple(self.connection.info().get('redis_version').split('.')[:2]))
                self._has_scripting = version >= 2.5 \
                    and hasattr(self.connection, 'register_script')
            except:
                self._has_scripting = False
        return self._has_scripting

    @property
    def prepared_middlewares(self):
        """
        Load, cache and return the list of usable middlewares, as a dict with
        an entry for each usable method.
        {
            'pre_command': [list, of, middlewares],
            'post_command': [list, of, middlewares],
        }
        Middlewares must be defined while declaring the database:
            database = RedisDatabase(middlewares=[
                AMiddleware(),
                AnoterMiddleware(some, parameter)
            ], **connection_settings)
        """

        if not hasattr(self, '_prepared_middlewares'):

            self._prepared_middlewares = {
                'pre_command': [],
                'post_command': [],
            }

            for middleware in self.middlewares:
                middleware.database = self

                for middleware_type in self._prepared_middlewares:
                    if hasattr(middleware, middleware_type):
                        self._prepared_middlewares[middleware_type].append(middleware)

            self._prepared_middlewares['post_command'] = self._prepared_middlewares['post_command'][::-1]

        return self._prepared_middlewares

    def run_command(self, command, context=None):
        """
        Run a redis command, passing it through all defined middlewares.
        The command must be a Command namedtuple
        """
        if context is None:
            context = {}

        result = None

        for middleware in self.prepared_middlewares['pre_command']:
            result = middleware.pre_command(command, context)
            if result:
                break

        if result is None:
            method = getattr(self.connection, "%s" % command.name)
            result = method(*command.args, **command.kwargs)

            if not isinstance(result, Result):
                result = Result(result)

        for middleware in self.prepared_middlewares['post_command']:
            result = middleware.post_command(command, result, context)

        return result.value
