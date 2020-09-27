from __future__ import annotations

import logging

import aioredis

__all__ = ['RedisSession', 'RedisSessionClosed', 'RedisSessionNotInitialized']

log = logging.getLogger(__name__)


class RedisSessionNotInitialized(RuntimeError):
    """Raised when the RedisSession instance has not been initialized yet."""


class RedisSessionClosed(RuntimeError):
    """Exception raised when something attempts operations on a closed redis session."""


class FakeRedisNotInstalled(ImportError):
    """Exception raised when trying to use `fakeredis` while it's not installed."""


class RedisSingleton(type):
    """
    Ensure that only a single RedisSession instance exists at a time.

    The metaclass first checks if an instance currently exists and, if so, it
    returns the existing instance. If an instance does not exist, it will create
    and return a new instance of the class.
    """

    def __init__(cls, *args) -> None:
        super().__init__(*args)
        cls._instance = None

    def __call__(cls, *args, **kwargs) -> RedisSession:
        """Return the singleton RedisSession instance."""
        if not cls._instance:
            cls._instance = super().__call__(*args, **kwargs)

        return cls._instance


class RedisSession(metaclass=RedisSingleton):
    """
    A RedisSession that manages the lifetime of a Redis client instance.

    To avoid making the client access more complicated than it needs to be for
    a client that should only be created once during the bot's runtime, the
    `RedisSession` instance should be created and its `connect` method should be
    awaited before the tasks get scheduled that rely on the Redis client.

    The easiest way to do that is to get the event loop before `discord.py`
    creates it, run the loop until the `connect` coroutine is completed, and
    passing the loop to `discord.ext.commands.Bot.__init__` afterwards.

    Example:
        redis_session = RedisSession()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(redis_session.connect(...))
        bot = discord.ext.commands.Bot(..., loop=loop)
    """

    _instance: RedisSession = None

    def __init__(
            self, *, global_namespace: str = "", use_fakeredis: bool = False, **session_kwargs
    ) -> None:
        self.global_namespace = global_namespace
        self._pool = None
        self._session_kwargs = session_kwargs
        self._use_fakeredis = use_fakeredis

    @classmethod
    def get_current_session(cls) -> RedisSession:
        """
        Get the currently connected RedisSession instance.

        If an instance has not been created yet as this point, this method will
        raise a `RedisSessionNotInitialized` exception.
        """
        if not cls._instance:
            raise RedisSessionNotInitialized("the redis session has not been initialized yet.")

        return cls._instance

    @property
    def pool(self) -> aioredis.Redis:
        """
        Get the connection pool after checking if it is still connected.

        This property will raise a `RedisSessionClosed` if it's accessed after
        the pool has been closed.
        """
        if self._pool.closed:
            raise RedisSessionClosed(
                "attempting to access connections pool after it has been closed."
            )

        return self._pool

    @property
    def closed(self) -> bool:
        """Return whether or not the RedisSession is closed."""
        return self._pool is None or self._pool.closed

    async def connect(self) -> None:
        """Connect to Redis by creating the connections pool."""
        log.debug("Creating Redis connections pool.")

        # Decide if we want to use `fakeredis` or an actual Redis server. The
        # option to use `fakeredis.aioredis` is provided to aid with running the
        # application in a development environment and to aid with writing
        # unittests.
        if self._use_fakeredis:
            # Only import fakeredis when required. This ensures that it's not a
            # required dependency if someone's not planning on using it.
            try:
                import fakeredis.aioredis
            except ImportError:
                raise FakeRedisNotInstalled(
                    "RedisSession was configured to use `fakeredis`, but it is not installed. "
                    "Either install `fakeredis` manually or install `async-rediscache` using "
                    "`pip install async-rediscache[fakeredis]` to enable support."
                )

            kwargs = dict(self._session_kwargs)

            # the `address` and `password` kwargs are not supported by redis
            kwargs.pop("address", None)
            kwargs.pop("password", None)

            pool_constructor = fakeredis.aioredis.create_redis_pool(**kwargs)
        else:
            pool_constructor = aioredis.create_redis_pool(**self._session_kwargs)

        self._pool = await pool_constructor

    async def close(self) -> None:
        """Close the pool and wait for pending operations to be processed."""
        self._pool.close()
        await self._pool.wait_closed()
