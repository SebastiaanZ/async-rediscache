import asyncio
import functools
import logging
from typing import Any, Callable

log = logging.getLogger(__name__)


class NamespaceLock(asyncio.Lock):
    """An asyncio.Lock subclass that is aware of the namespace that it's locking."""

    def __init__(self, namespace: str) -> None:
        super().__init__()
        self._namespace = namespace

    def __repr__(self) -> str:
        """Create an insightful representation for this NamespaceLock object."""
        status = "locked" if self.locked() else "unlocked"
        cls = self.__class__.__name__
        return f"<{cls} namespace={self._namespace!r} [{status}]>"


def namespace_lock(method: Callable) -> Callable:
    """Atomify the decorated method from a Redis perspective."""
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
                self._namespace_locks[namespace] = NamespaceLock(namespace=namespace)

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
