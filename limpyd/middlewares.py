# -*- coding:utf-8 -*-

from time import time

from limpyd.exceptions import ImplementationError


class BaseMiddleware(object):
    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, value):
        if hasattr(self, '_database'):
            raise ImplementationError("Cannot change the database of a middleware")
        self._database = value

    # minimal pre_command method: do nothing and return None
    # def pre_command(self, command, context):
    #     pass

    # minimal post_command method: return the given result
    # def post_command(self, command, result, context):
    #     return result


class LoggingMiddleware(BaseMiddleware):
    """
    Middleware that takes a logger, and log commands and their result (and time
    to run the command).
    """
    def __init__(self, logger, log_results=True, log_time=True):
        """
        The logger must be a defined and correctly initialized one (via logging)
        The log_results flag indicates if only the commands or also their result
        (with duration, if log_time is True) are logged.
        """
        self.logger = logger
        self.log_results = log_results
        self.log_time = log_time
        super(LoggingMiddleware, self).__init__()

    @BaseMiddleware.database.setter
    def database(self, value):
        BaseMiddleware.database.fset(self, value)  # super
        self.database._command_logger_counter = 0

    def pre_command(self, command, context):
        self.database._command_logger_counter += 1
        context['_command_number'] = self.database._command_logger_counter
        if self.log_time:
            context['_start_time'] = time()
        self.logger.info(u'[#%s] %s' % (context['_command_number'], str(command)))

    def post_command(self, command, result, context):
        if self.log_results:
            log_str = u'[#%s] %s'
            log_params = [context['_command_number'], str(result)]
            if self.log_time:
                log_str = u'[#%s, in %0.0fµs] %s'
                duration = (time() - context['_start_time']) * 1000000
                log_params.insert(1, duration)

            self.logger.info(log_str % tuple(log_params))
        return result
