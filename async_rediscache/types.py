from __future__ import annotations

import asyncio
import functools
import logging
from functools import partialmethod
from types import MethodType
from typing import Any, Callable, Dict, ItemsView, Optional, Tuple, Union

import aioredis

from .helpers import namespace_lock
from .session import RedisSession

__all__ = [
    "NoNamespaceError",
    "RedisCache",
    "RedisKeyOrValue",
    "RedisKeyType",
    "RedisValueType",
    "RedisQueue"
]

log = logging.getLogger(__name__)

# Type aliases
RedisKeyType = Union[str, int]
RedisValueType = Union[str, int, float, bool]
RedisKeyOrValue = Union[RedisKeyType, RedisValueType]

# Prefix tuples
_PrefixTuple = Tuple[Tuple[str, Any], ...]
_VALUE_PREFIXES = (
    ("f|", float),
    ("i|", int),
    ("s|", str),
    ("b|", bool),
)
_KEY_PREFIXES = (
    ("i|", int),
    ("s|", str),
)


class NoNamespaceError(RuntimeError):
    """Raised when a RedisCache instance has no namespace."""


class RedisObject:
    """A base class for Redis caching object implementations."""

    _namespace_locks = {}

    def __init__(
            self, *, namespace: Optional[str] = None, use_global_namespace: bool = True
    ) -> None:
        """Initialize the RedisCache."""
        self._local_namespace = namespace
        self._use_global_namespace = use_global_namespace
        self._transaction_lock = None

    def __set_name__(self, owner: Any, attribute_name: str) -> None:
        """
        Set the namespace to Class.attribute_name.

        Called automatically when this class is assigned to a class attribute.

        This class MUST be created as a class attribute, otherwise it will raise
        exceptions whenever a method is used. This is because it uses this
        method to create a namespace like `MyCog.my_class_attribute` which is
        used as a hash name when we store stuff in Redis, to prevent collisions.

        The namespace is only set the first time a class attribute gets assigned
        to a RedisCache instance. Assigning a class attribute to an existing
        instance will not overwrite the namespace and the additional class
        attribute will act as an alias to the original instance.
        """
        if not self._local_namespace:
            self._local_namespace = f"{owner.__name__}.{attribute_name}"

    def __repr__(self) -> str:
        """Return a beautiful representation of this object instance."""
        return f"{self.__class__.__name__}(namespace={self._local_namespace!r})"

    @property
    def redis_session(self) -> RedisSession:
        """Get the current active RedisSession."""
        return RedisSession.get_current_session()

    @property
    def namespace(self) -> str:
        """Return the `namespace` of this RedisObject."""
        global_namespace = self.redis_session.global_namespace
        if self._use_global_namespace and global_namespace:
            namespace = f"{global_namespace}.{self._local_namespace}"
        else:
            namespace = self._local_namespace

        return namespace

    async def _get_pool_connection(self) -> aioredis.commands.ContextRedis:
        """Get a connection from the pool after validating a namespace was set."""
        if self._local_namespace is None:
            cls_name = self.__class__.__name__
            error_message = (
                f"can't get a pool connection as the {cls_name} instance does not have a namespace."
            )
            log.critical(error_message)
            raise NoNamespaceError(error_message)

        return await self.redis_session.pool

    @staticmethod
    def _to_typestring(key_or_value: RedisKeyOrValue, prefixes: _PrefixTuple) -> str:
        """Turn a valid Redis type into a typestring."""
        key_or_value_type = type(key_or_value)

        for prefix, _type in prefixes:
            # isinstance is a bad idea here, because isinstance(False, int) == True.
            if key_or_value_type is _type:
                if key_or_value_type is bool:
                    # Convert bools into integers before storing them
                    key_or_value = int(key_or_value)

                return f"{prefix}{key_or_value}"

        raise TypeError(f"RedisObject._to_typestring only supports the following: {prefixes}.")

    @staticmethod
    def _from_typestring(
            key_or_value: Union[bytes, str], prefixes: _PrefixTuple
    ) -> RedisKeyOrValue:
        """Deserialize a typestring into a valid Redis type."""
        # Stuff that comes out of Redis will be bytestrings, so let's decode those.
        if isinstance(key_or_value, bytes):
            key_or_value = key_or_value.decode('utf-8')

        # Now we convert our unicode string back into the type it originally was.
        for prefix, _type in prefixes:
            if key_or_value.startswith(prefix):

                # For booleans, we need special handling because bool("False") is True.
                if prefix == "b|":
                    value = key_or_value[len(prefix):]
                    return bool(int(value))

                # Otherwise we can just convert normally.
                return _type(key_or_value[len(prefix):])
        raise TypeError(f"RedisObject._from_typestring only supports the following: {prefixes}.")

    # Add some nice partials to call our generic typestring converters.
    # These are basically methods that will fill in some of the parameters for you, so that
    # any call to _key_to_typestring will be like calling _to_typestring with the two parameters
    # at `prefixes` and `types_string` pre-filled.
    #
    # See https://docs.python.org/3/library/functools.html#functools.partialmethod
    _key_to_typestring: MethodType = partialmethod(_to_typestring, prefixes=_KEY_PREFIXES)
    _value_to_typestring: MethodType = partialmethod(_to_typestring, prefixes=_VALUE_PREFIXES)
    _key_from_typestring: MethodType = partialmethod(_from_typestring, prefixes=_KEY_PREFIXES)
    _value_from_typestring: MethodType = partialmethod(_from_typestring, prefixes=_VALUE_PREFIXES)

    def _dict_from_typestring(self, dictionary: Dict) -> Dict:
        """Turns all contents of a dict into valid Redis types."""
        return {
            self._key_from_typestring(key): self._value_from_typestring(value)
            for key, value in dictionary.items()
        }

    def _dict_to_typestring(self, dictionary: Dict) -> Dict:
        """Turns all contents of a dict into typestrings."""
        return {
            self._key_to_typestring(key): self._value_to_typestring(value)
            for key, value in dictionary.items()
        }

    def atomic_transaction(self, method: Callable) -> Callable:
        """
        Ensure that the decorated method is atomic within a RedisObject.

        Some operations performed on a RedisObject need to occur atomically,
        from the perspective of Redis. An example is trying to set multiple
        values that form a consistent set and should be set "all at once".

        By applying this decorator to all methods that interact with those
        consistent sets, those methods need to acquire a lock before they are
        allowed to run. This means that these methods will "wait" for the
        previous tasks to be finished.

        The `asyncio.Lock` is RedisObject-specific, meaning that there's a
        separate lock for each RedisObject (e.g., a RedisCache).

        The `wrapper` lazily loads the `asyncio.Lock` to ensure it's created
        within the right running event loop.

        Note: Take care not to await decorated method from within a method also
        decorated by this decorator. It will cause a deadlock...
        """
        log.debug(f"Wrapping {method.__qualname__} to ensure atomic transactions")

        @functools.wraps(method)
        async def wrapper(*args, **kwargs) -> Any:
            if self._transaction_lock is None:
                log.debug(f"Creating a transaction lock for {self!r}")
                self._transaction_lock = asyncio.Lock()

            log.debug(f"[transaction lock] {method.__qualname__}: Trying to acquire lock")
            async with self._transaction_lock:
                log.debug(f"[transaction lock] {method.__qualname__}: Acquired lock")
                result = await method(*args, **kwargs)

            log.debug(f"[transaction lock] {method.__qualname__}: Released lock")
            return result

        return wrapper


class RedisCache(RedisObject):
    """
    A simplified interface for a Redis hash set.

    We implement several convenient methods that are fairly similar to have a
    dict behaves, and should be familiar to Python users. The biggest difference
    is that all the public methods in this class are coroutines, and must be
    awaited.

    Because of limitations in Redis, this cache will only accept strings and
    integers as keys and strings, integers, floats, and bools as values.

    By default, the namespace key of a RedisCache is automatically determined
    by the name of the owner class and the class attribute assigned to the
    RedisQueue instance. To bind a RedisQueue to a specific namespace, pass the
    namespace as the `namespace` keyword argument to constructor.

    Please note that for automatic namespacing, this class MUST be created as a
    class attribute to properly initialize the instance's namespace. See
    `__set_name__` for more information about how this works.

    Simple example for how to use this:

    class SomeCog(Cog):
        # To initialize a valid RedisCache, just add it as a class attribute
        # here. Do not add it to the __init__ method or anywhere else, it MUST
        # be a class attribute. Do not pass any parameters.
        cache = RedisCache()

        async def my_method(self):

            # Now we're ready to use the RedisCache.
            #
            # We can store some stuff in the cache just by doing this.
            # This data will persist through restarts!
            await self.cache.set("key", "value")

            # To get the data, simply do this.
            value = await self.cache.get("key")

            # Other methods work more or less like a dictionary.
            # Checking if something is in the cache
            await self.cache.contains("key")

            # iterating the cache
            async for key, value in self.cache.items():
                print(value)

            # We can even iterate in a comprehension!
            consumed = [value async for key, value in self.cache.items()]
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the RedisCache."""
        super().__init__(*args, **kwargs)
        self._increment_lock = None

    @namespace_lock
    async def set(self, key: RedisKeyType, value: RedisValueType) -> None:
        """Store an item in the Redis cache."""
        # Convert to a typestring and then set it
        key = self._key_to_typestring(key)
        value = self._value_to_typestring(value)

        log.debug(f"Setting {key} to {value}.")
        with await self._get_pool_connection() as connection:
            await connection.hset(self.namespace, key, value)

    @namespace_lock
    async def get(
            self, key: RedisKeyType, default: Optional[RedisValueType] = None
    ) -> Optional[RedisValueType]:
        """Get an item from the Redis cache."""
        key = self._key_to_typestring(key)

        log.debug(f"Attempting to retrieve {key}.")
        with await self._get_pool_connection() as connection:
            value = await connection.hget(self.namespace, key)

        if value is None:
            log.debug(f"Value not found, returning default value {default}")
            return default
        else:
            value = self._value_from_typestring(value)
            log.debug(f"Value found, returning value {value}")
            return value

    @namespace_lock
    async def delete(self, key: RedisKeyType) -> None:
        """
        Delete an item from the Redis cache.

        If we try to delete a key that does not exist, it will simply be ignored.

        See https://redis.io/commands/hdel for more info on how this works.
        """
        key = self._key_to_typestring(key)

        log.debug(f"Attempting to delete {key}.")
        with await self._get_pool_connection() as connection:
            return await connection.hdel(self.namespace, key)

    @namespace_lock
    async def contains(self, key: RedisKeyType) -> bool:
        """
        Check if a key exists in the Redis cache.

        Return True if the key exists, otherwise False.
        """
        key = self._key_to_typestring(key)
        with await self._get_pool_connection() as connection:
            exists = await connection.hexists(self.namespace, key)

        log.debug(f"Testing if {key} exists in the RedisCache - Result is {exists}")
        return exists

    @namespace_lock
    async def items(self) -> ItemsView:
        """
        Fetch all the key/value pairs in the cache.

        Returns a normal ItemsView, like you would get from dict.items().

        Keep in mind that these items are just a _copy_ of the data in the
        RedisCache - any changes you make to them will not be reflected
        into the RedisCache itself. If you want to change these, you need
        to make a .set call.

        Example:
        items = await my_cache.items()
        for key, value in items:
            # Iterate like a normal dictionary
        """
        with await self._get_pool_connection() as connection:
            items = self._dict_from_typestring(await connection.hgetall(self.namespace)).items()

        log.debug(f"Retrieving all key/value pairs from cache, total of {len(items)} items.")
        return items

    @namespace_lock
    async def length(self) -> int:
        """Return the number of items in the Redis cache."""
        with await self._get_pool_connection() as connection:
            number_of_items = await connection.hlen(self.namespace)
        log.debug(f"Returning length. Result is {number_of_items}.")
        return number_of_items

    @namespace_lock
    async def to_dict(self) -> Dict:
        """Convert to dict and return."""
        return {key: value for key, value in await self.items(acquire_lock=False)}

    @namespace_lock
    async def clear(self) -> None:
        """Deletes the entire hash from the Redis cache."""
        log.debug("Clearing the cache of all key/value pairs.")
        with await self._get_pool_connection() as connection:
            await connection.delete(self.namespace)

    @namespace_lock
    async def pop(
            self, key: RedisKeyType, default: Optional[RedisValueType] = None
    ) -> RedisValueType:
        """Get the item, remove it from the cache, and provide a default if not found."""
        log.debug(f"Attempting to pop {key}.")
        value = await self.get(key, default, acquire_lock=False)

        log.debug(
            f"Attempting to delete item with key '{key}' from the cache. "
            "If this key doesn't exist, nothing will happen."
        )
        await self.delete(key, acquire_lock=False)

        return value

    @namespace_lock
    async def update(self, items: Dict[RedisKeyType, RedisValueType]) -> None:
        """
        Update the Redis cache with multiple values.

        This works exactly like dict.update from a normal dictionary. You pass
        a dictionary with one or more key/value pairs into this method. If the
        keys do not exist in the RedisCache, they are created. If they do exist,
        the values are updated with the new ones from `items`.

        Please note that keys and the values in the `items` dictionary
        must consist of valid RedisKeyTypes and RedisValueTypes.
        """
        log.debug(f"Updating the cache with the following items:\n{items}")
        with await self._get_pool_connection() as connection:
            await connection.hmset_dict(self.namespace, self._dict_to_typestring(items))

    @namespace_lock
    async def increment(self, key: RedisKeyType, amount: Optional[int, float] = 1) -> None:
        """
        Increment the value by `amount`.

        This works for both floats and ints, but will raise a TypeError
        if you try to do it for any other type of value.

        This also supports negative amounts, although it would provide better
        readability to use .decrement() for that.
        """
        log.debug(f"Attempting to increment/decrement the value with the key {key} by {amount}.")

        value = await self.get(key, acquire_lock=False)

        # Can't increment a non-existing value
        if value is None:
            error_message = "The provided key does not exist!"
            log.error(error_message)
            raise KeyError(error_message)

        # If it does exist and it's an int or a float, increment and set it.
        if isinstance(value, int) or isinstance(value, float):
            value += amount
            await self.set(key, value, acquire_lock=False)
        else:
            error_message = "You may only increment or decrement integers and floats."
            log.error(error_message)
            raise TypeError(error_message)

    async def decrement(self, key: RedisKeyType, amount: Optional[int, float] = 1) -> None:
        """
        Decrement the value by `amount`.

        Basically just does the opposite of .increment.
        """
        await self.increment(key, -amount)


class RedisQueue(RedisObject):
    """
    A Redis-backed queue that can be used for producer/consumer design patterns.

    The queue is backed internally by a Redis List, which allows you to append
    to and pop from both sides of the queue in constant time. To avoid confusion
    about the endianness of queue, the queue uses methods such as `put` and
    `get` instead of `lpop` and `rpush`; this is similar to the interface
    provided by `Queue.SimpleQueue` in Python's standard library.

    By default, the namespace key of a RedisQueue is automatically determined
    by the name of the owner class and the class attribute assigned to the
    RedisQueue instance. To bind a RedisQueue to a specific namespace, pass the
    namespace as the `namespace` keyword argument to constructor.
    """

    @namespace_lock
    async def put(self, value: RedisValueType) -> None:
        """
        Remove and return a value from the queue.

        If `wait` is `True` (default), this method will wait for an item to
        become available. An optional `timeout` specifies for how long this
        method should wait. A `timeout` value of `0` indicates that this method
        will wait forever.

        This method returns `None` if no item was available within the bounds of
        the specified waiting conditions.
        """
        value_string = self._value_to_typestring(value)
        log.debug(f"putting {value_string!r} on RedisQueue `{self.namespace}`")
        with await self._get_pool_connection() as connection:
            await connection.rpush(self.namespace, value_string)

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    put_nowait = partialmethod(put)

    @namespace_lock
    async def get(self, wait: bool = True, timeout: int = 0) -> Optional[RedisValueType]:
        """
        Remove and return a value from the queue.

        If `wait` is `True` (default), this method will wait for an item to
        become available. An optional `timeout` specifies for how long this
        method should wait. A `timeout` value of `0` indicates that this method
        will wait forever.

        This method returns `None` if no item was available within the waiting
        conditions specified.
        """
        log.debug(
            f"getting value from RedisQueue `{self.namespace}` "
            f"(wait={wait!r}, timeout={timeout!r})"
        )

        with await self._get_pool_connection() as connection:
            if wait:
                value = await connection.blpop(self.namespace, timeout=timeout)

                # If we can get an item from the queue before the timeout runs
                # out, we get a list back, in the form `[namespace, value]`. If
                # no value was received before the timeout, we simply get `None`
                # back. This means we need to get the value out of the list when
                # we actually got a value back instead of `None`.
                if value:
                    _, value = value
            else:
                value = await connection.lpop(self.namespace)

        if value is not None:
            value = self._value_from_typestring(value)

        log.debug(f"got value `{value!r}` from RedisQueue `{self.namespace}`")
        return value

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    get_nowait = partialmethod(get, wait=False)

    @namespace_lock
    async def qsize(self) -> int:
        """
        Return the (approximate) size of the RedisQueue.

        Note that while we can determine the exact size of the queue at the
        moment Redis receives the request, this value may have become stale
        before we received it back.
        """
        with await self._get_pool_connection() as connection:
            return await connection.llen(self.namespace)

    @namespace_lock
    async def empty(self) -> bool:
        """
        Return `True` if the RedisQueue is empty.

        The caveat that applies to the `qsize` method also applies here.
        """
        return await self.qsize(acquire_lock=False) == 0
