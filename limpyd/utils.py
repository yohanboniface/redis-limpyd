import uuid

from functools import wraps
from logging import getLogger

log = getLogger(__name__)


def make_key(*args):
    """Create the key concatening all args with `:`."""
    return u":".join(unicode(arg) for arg in args)


def unique_key(connection):
    """
    Generate a unique keyname that does not exists is the connection
    keyspace.
    """
    while 1:
        key = uuid.uuid4().hex
        if not connection.exists(key):
            break
    return key


def make_cache_key(*args, **kwargs):
    return frozenset(args + tuple(kwargs.items()))


class memoize_command(object):
    def __call__(self, func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # self here is a field instance

            if not self.cacheable or self.database.discard_cache:
                return func(self, *args, **kwargs)

            haxh = make_cache_key(*args, **kwargs)

            # Cache per field name to be able to flush per field
            if not self.has_cache():
                self.init_cache()
            field_cache = self.get_cache()
            # Warning: Some commands are both setter and modifiers (getset)
            command_name = args[0]
            if command_name in self.available_modifiers:
                # clear cache each time a modifier affects the field
                log.debug("Clearing cache for %s" % self.name)
                field_cache.clear()
            if haxh not in field_cache:
                # Run command and store result
                # It will be run only first time for getters and every time for
                # modifiers
                result = func(self, *args, **kwargs)
                if command_name in self.available_getters:
                    # Populate the cache if getter
                    log.debug("Storing key %s for %s" % (haxh, self.name))
                    field_cache[haxh] = result
            else:
                result = field_cache[haxh]
            return result
        return wrapper
