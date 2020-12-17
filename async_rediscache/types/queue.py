import functools
import logging
from typing import Optional

from .base import RedisObject, RedisValueType, namespace_lock_no_warn

__all__ = [
    "RedisQueue",
]

log = logging.getLogger(__name__)


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

    @namespace_lock_no_warn
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
            await connection.lpush(self.namespace, value_string)

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    put_nowait = functools.partialmethod(put)

    @namespace_lock_no_warn
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
                value = await connection.brpop(self.namespace, timeout=timeout)

                # If we can get an item from the queue before the timeout runs
                # out, we get a list back, in the form `[namespace, value]`. If
                # no value was received before the timeout, we simply get `None`
                # back. This means we need to get the value out of the list when
                # we actually got a value back instead of `None`.
                if value:
                    _, value = value
            else:
                value = await connection.rpop(self.namespace)

        if value is not None:
            value = self._value_from_typestring(value)

        log.debug(f"got value `{value!r}` from RedisQueue `{self.namespace}`")
        return value

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    get_nowait = functools.partialmethod(get, wait=False)

    @namespace_lock_no_warn
    async def qsize(self) -> int:
        """
        Return the (approximate) size of the RedisQueue.

        Note that while we can determine the exact size of the queue at the
        moment Redis receives the request, this value may have become stale
        before we received it back.
        """
        with await self._get_pool_connection() as connection:
            return await connection.llen(self.namespace)

    @namespace_lock_no_warn
    async def empty(self) -> bool:
        """
        Return `True` if the RedisQueue is empty.

        The caveat that applies to the `qsize` method also applies here.
        """
        return await self.qsize(acquire_lock=False) == 0
