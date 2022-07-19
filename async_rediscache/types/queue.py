from __future__ import annotations

import functools
import logging
import typing
import weakref
from typing import Optional

import aioredis

from .base import RedisObject, RedisValueType

__all__ = [
    "RedisQueue",
    "RedisTaskQueue",
    "RedisTask",
    "TaskAlreadyDone",
    "TaskNotPending",
]

log = logging.getLogger(__name__)


class TaskAlreadyDone(RuntimeError):
    """Raised when finalized is called on a task that is already done."""


class TaskNotPending(RuntimeError):
    """Raised when finalizing a task that is not found in the 'pending' queue."""


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
        await self.redis_session.client.lpush(self.namespace, value_string)

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    put_nowait = functools.partialmethod(put)

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

        if wait:
            value = await self.redis_session.client.brpop(self.namespace, timeout=timeout)

            # If we can get an item from the queue before the timeout runs
            # out, we get a list back, in the form `[namespace, value]`. If
            # no value was received before the timeout, we simply get `None`
            # back. This means we need to get the value out of the list when
            # we actually got a value back instead of `None`.
            if value:
                _, value = value
        else:
            value = await self.redis_session.client.rpop(self.namespace)

        if value is not None:
            value = self._value_from_typestring(value)

        log.debug(f"got value `{value!r}` from RedisQueue `{self.namespace}`")
        return value

    # This method is provided to provide a compatible interface with Queue.SimpleQueue
    get_nowait = functools.partialmethod(get, wait=False)

    async def qsize(self) -> int:
        """
        Return the (approximate) size of the RedisQueue.

        Note that while we can determine the exact size of the queue at the
        moment Redis receives the request, this value may have become stale
        before we received it back.
        """
        return await self.redis_session.client.llen(self.namespace)

    async def empty(self) -> bool:
        """
        Return `True` if the RedisQueue is empty.

        The caveat that applies to the `qsize` method also applies here.
        """
        return await self.qsize() == 0

    async def iter_tasks(
            self, wait: bool = True, timeout: int = 0
    ) -> typing.AsyncGenerator[typing.Union[RedisValueType, RedisTask], None, None]:
        """Yield all items the queue, optionally waiting for new tasks."""
        while True:
            value = await self.get(wait, timeout)
            if value is None:
                return

            yield value

    def __aiter__(
            self
    ) -> typing.AsyncGenerator[typing.Union[RedisValueType, RedisTask], None, None]:
        """Yield all items in the queue until it's emptied."""
        return self.iter_tasks(wait=False)


class RedisTaskQueue(RedisQueue):
    """A Queue class with task tracking features to prevent data loss."""

    def __init__(self, *args, client_identifier: typing.Optional[str] = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.client_identifier = client_identifier

    @property
    def namespace_pending(self) -> str:
        """Get the name of the queue where pending tasks are stored."""
        client_id = f"{self.client_identifier}_" if self.client_identifier is not None else ""
        return f"{self.namespace}${client_id}pending"

    async def get(self, wait: bool = True, timeout: int = 0) -> Optional[RedisTask]:
        """
        Get an item from the queue wrapped in a Task instance.

        When you get an item from the queue, it will not be directly removed
        from Redis. Instead, it will be moved to an in-progress list to prevent
        data loss in case the worker is interrupted for its job is completed.

        You should mark a `Task` as done by calling its `finalize` method in the
        worker once it has completed its work to prevent items from staying
        alive indefinitely. See the `Task` class for more information.
        """
        log.debug(
            f"getting value from RedisTaskQueue `{self.namespace}` "
            f"(wait={wait!r}, timeout={timeout!r})"
        )

        namespaces = {"src": self.namespace, "dst": self.namespace_pending}
        if wait:
            value = await self.redis_session.client.brpoplpush(**namespaces, timeout=timeout)
        else:
            value = await self.redis_session.client.rpoplpush(**namespaces)

        if value is not None:
            value = self._value_from_typestring(value)
            value = RedisTask(value, owner=self)

        log.debug(f"got value `{value!r}` from RedisTaskQueue `{self.namespace}`")
        return value

    async def task_done(self, task: RedisTask) -> None:
        """Mark a task as done by removing it from the pending tasks queue."""
        typestring = self._value_to_typestring(task.value)
        removed = await self.redis_session.client.lrem(self.namespace_pending, 1, typestring)

        if not removed:
            raise TaskNotPending(f"task {task.value!r} was not found in the pending tasks queue.")

        task.done = True

    async def reschedule_pending_task(self, task: typing.Union[RedisValueType. RedisTask]) -> None:
        """
        Move a `task` from the pending tasks queue back to the main queue.

        This is a DANGEROUS operation: Rescheduling a task that is currently
        still being processed by a worker leads to an inconsistent state: The
        task is still being processed, but it's also queued to be processed
        again. It will also trigger a `RuntimeError` when the worker attempts
        to mark the task as done as the task will not be found in the pending
        tasks queue.
        """
        if isinstance(task, RedisTask):
            task = task.value

        reschedule_script = await self._load_script("redisqueue_reschedule_task.lua")

        try:
            keys = [self.namespace, self.namespace_pending]
            args = [self._value_to_typestring(task)]
            await self.redis_session.client.evalsha(reschedule_script, len(keys), *keys, *args)
        except aioredis.ResponseError:
            raise TaskNotPending(
                f"task `{task}` not found in pending tasks queue `{self.namespace_pending}`"
            ) from None

    async def reschedule_all_pending_client_tasks(self) -> int:
        """
        Reschedule all pending tasks of this client.

        This is a DANGEROUS operation that could lead to an inconsistent state
        in the queue. See `RedisTaskQueue.reschedule_pending_task` for more
        information.
        """
        reschedule_script = await self._load_script("redisqueue_reschedule_all_client_tasks.lua")
        rescheduled_tasks = await self.redis_session.client.evalsha(
            reschedule_script,
            2,
            self.namespace,
            self.namespace_pending,
        )

        return int(rescheduled_tasks)


class RedisTask:
    """
    A class that represents a task popped from a RedisQueue.

    A task has a weak reference to its owner queue, which means you can mark a
    task as done as long as the owner queue is still alive.
    """

    def __init__(self, value: RedisValueType, owner: RedisTaskQueue) -> None:
        self._value = value
        self.owner_reference = weakref.ref(owner)
        self.done = False

    def __repr__(self) -> str:
        """Return the official representation of the task."""
        cls = self.__class__.__name__
        status = "done" if self.done else "pending"
        return f"<{cls} task_data={self._value!r} [{status}]>"

    @property
    def value(self) -> RedisValueType:
        """Return the task value."""
        return self._value

    @property
    def owner(self) -> RedisTaskQueue:
        """Get the owner RedisTaskQueue from the weak reference."""
        queue = self.owner_reference()
        if not queue:
            raise RuntimeError("can't finalize task as the queue instance no longer exists")

        return queue

    async def finalize(self) -> None:
        """Mark the task as done and remove it from the pending tasks queue."""
        if self.done:
            raise TaskAlreadyDone("task was already marked as done")

        await self.owner.task_done(task=self)

    async def reschedule(self) -> None:
        """Reschedule this task in the main queue."""
        await self.owner.reschedule_pending_task(self)
