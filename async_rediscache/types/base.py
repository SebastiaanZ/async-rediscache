from __future__ import annotations

import asyncio
import datetime
import functools
import importlib.resources
import logging
import warnings
from functools import partialmethod
from types import MethodType
from typing import Any, Callable
from typing import Dict, Optional, Tuple, Union

import aioredis

from ..session import RedisSession

__all__ = [
    "RedisObject",
    "NoNamespaceError",
    "RedisKeyOrValue",
    "RedisKeyType",
    "RedisValueType",
    "namespace_lock",
    "namespace_lock_no_warn",
    "NamespaceLock",
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
_ERROR_PREFIXES = (
    ("TypeError|", TypeError),
    ("ValueError|", ValueError),
)


class NoNamespaceError(RuntimeError):
    """Raised when a RedisCache instance has no namespace."""


class NamespaceLock(asyncio.Lock):
    """
    An asyncio.Lock subclass that is aware of the namespace that it's locking.

    This class is DEPRECATED and will be removed in version 1.0.0. See the
    docstring of the `namespace_lock` decorator for more information.
    """

    def __init__(self, namespace: str, *, warn: bool = True) -> None:
        if warn:
            warnings.warn(
                "The use of `NamespaceLock` is deprecated. It will be removed "
                "entirely in version 1.0.0.",
                DeprecationWarning
            )

        super().__init__()
        self._namespace = namespace

    def __repr__(self) -> str:
        """Create an insightful representation for this NamespaceLock object."""
        status = "locked" if self.locked() else "unlocked"
        cls = self.__class__.__name__
        return f"<{cls} namespace={self._namespace!r} [{status}]>"


def namespace_lock(method: Callable, *, warn: bool = True) -> Callable:
    """
    Atomify the decorated method from a Redis perspective.

    Namespace locks are DEPRECATED. The issue with namespace locks is that it is
    very easy to implement something that will deadlock. They were used to wrap
    compound methods that had to issue multiple Redis Commands to do their work.

    The downside is that if one method uses another method internally, it needs
    to make sure that the other method does not try to acquire the lock, as the
    calling method already has it. If not, a deadlock will occur where the
    method called internally is waiting for a lock that's acquired by its
    caller, while the caller will only release the lock after the work is done.

    Another downside is that in a multiple client setup, such a lock is specific
    to a single client. This means that while atomicity is pseudo-guaranteed
    within a client, race conditions between clients still occur.

    To solve the issue, async-rediscache has switched to an approach using Redis
    scripting that brings the atomicity of the entire operation to the side of
    Redis again.

    This decorator will be removed in version 1.0.0, but it's kept around with
    a deprecation warning until it's removed.
    """
    if warn:
        warnings.warn(
            "The `namespace_lock` decorator is deprecated. It will be removed "
            "completely in version 1.0.0.",
            DeprecationWarning
        )

    @functools.wraps(method)
    async def wrapper(self, *args, acquire_lock: bool = True, **kwargs) -> Any:  # noqa: ANN001
        """
        Wrap the method in a function that automatically acquires a NamespaceLock.

        If `acquire_lock` is `False`, acquiring the lock will be skipped. This
        allows a compound method to call other methods without triggering a
        deadlock situation.
        """
        coroutine_object = method(self, *args, **kwargs)
        if acquire_lock:
            # Get fully qualified namespace to fetch the correct lock
            namespace = self.namespace

            # Check if we already have a lock for namespace; if not, create it.
            if namespace not in self._namespace_locks:
                log.debug(f"Creating NamespaceLock for {namespace=}.")
                self._namespace_locks[namespace] = NamespaceLock(
                    namespace=namespace, warn=warn
                )

            # Get the lock for this namespace
            lock = self._namespace_locks[namespace]

            # Acquire lock
            log.debug(f"Trying to acquire {lock} for {method.__qualname__}")
            async with lock:
                log.debug(f"Acquired {lock} for {method.__qualname__}")
                result = await coroutine_object
            log.debug(f"Released {lock} for {method.__qualname__}")
        else:
            result = await coroutine_object

        return result

    return wrapper


namespace_lock_no_warn = functools.partial(namespace_lock, warn=False)


class RedisObject:
    """A base class for Redis caching object implementations."""

    _namespace_locks = {}
    _registered_scripts = {}

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

    def _maybe_value_from_typestring(
        self,
        maybe_value: Optional[Union[bytes, str]],
        default: Optional[RedisValueType] = None,
    ) -> Optional[RedisValueType]:
        """
        Deserialize an optional redis return value safely.

        This method will try to match `maybe_value` in three ways:
        - If `maybe_value` is `None`, return the default
        - If `maybe_value` represents an error, raise the appropriate exception
        - If `maybe_value` represents a valid return value, deserialize it
        """
        if maybe_value is None:
            return default

        if isinstance(maybe_value, bytes):
            maybe_value = maybe_value.decode('utf-8')

        for prefix, exception in _ERROR_PREFIXES:
            if maybe_value.startswith(prefix):
                raise exception(maybe_value[len(prefix):])

        return self._value_from_typestring(maybe_value)

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

    async def _load_script(self, script: str) -> str:
        """Load a Redis Lua script and return the SHA Digest."""
        if script in self._registered_scripts:
            digest = self._registered_scripts[script]

            # check if the script is already registered with redis
            with await self._get_pool_connection() as connection:
                [script_exists] = await connection.script_exists(digest)

            if script_exists:
                return digest

        redis_script = importlib.resources.read_text("async_rediscache.redis_scripts", script)
        log.debug(f"Registering `{script}` script with Redis.")

        with await self._get_pool_connection() as connection:
            self._registered_scripts[script] = await connection.script_load(redis_script)

        return self._registered_scripts[script]

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

    @namespace_lock_no_warn
    async def set_expiry(self, seconds: float) -> bool:
        """
        Set a time-to-live on the entire RedisCache namespace.

        This method accepts a precision down to 1 ms. If more decimal
        places are provided, the duration is truncated. Passing a
        negative expire will result in the namespace being deleted
        immediately.

        Note: Setting an expiry on a key within the namespace is not
        supported by Redis. It's the entire namespace or nothing.
        """
        with await self._get_pool_connection() as connection:
            result = await connection.pexpire(self.namespace, int(1000*seconds))

        return bool(result)

    @namespace_lock_no_warn
    async def set_expiry_at(self, timestamp: Union[datetime.datetime, float]) -> bool:
        """
        Set a specific timestamp for the entire RedisCache to expire.

        This method accepts either a `datetime.datetime` or seconds since
        the Unix Epoch with a maximum precision of four decimal places (ms).

        Note: Setting an expiry on a key within the namespace is not
        supported by Redis. It's the entire namespace or nothing.
        """
        if isinstance(timestamp, datetime.datetime):
            timestamp = timestamp.timestamp()

        with await self._get_pool_connection() as connection:
            result = await connection.pexpireat(self.namespace, int(1000*timestamp))

        return bool(result)
