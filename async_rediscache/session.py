from __future__ import annotations

import logging
import warnings

import redis.asyncio

__all__ = ['RedisSession', 'RedisSessionNotInitialized', 'RedisSessionNotConnected']

log = logging.getLogger(__name__)


class RedisSessionNotInitialized(RuntimeError):
    """Raised when the RedisSession instance has not been initialized yet."""


class RedisSessionNotConnected(RuntimeError):
    """Raised when trying to access the Redis client before `connect` has been called."""


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
    a client that should only be created once during the application's runtime, the
    `RedisSession` instance should be created and its `connect` method should be
    awaited before the tasks get scheduled that rely on the Redis client.

    If using a third party library which interacts with the event loop, obtain it before it's
    created, and run the loop until the `connect` coroutine is completed.
    Pass the loop to library.

    Example:
        redis_session = RedisSession(host="localhost", port=6379)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(redis_session.connect())
        bot = discord.ext.commands.Bot(..., loop=loop)
    """

    _instance: RedisSession = None

    def __init__(
        self, *, global_namespace: str = "", use_fakeredis: bool = False, **session_kwargs
    ) -> None:
        self.global_namespace = global_namespace

        self._client = None
        self._session_kwargs = session_kwargs
        self._use_fakeredis = use_fakeredis
        self.valid = False

    @classmethod
    def get_current_session(cls) -> RedisSession:
        """
        Get the currently configured RedisSession instance.

        If an instance has not been created yet as this point, this method will
        raise a `RedisSessionNotInitialized` exception.
        """
        if not cls._instance:
            raise RedisSessionNotInitialized("the redis session has not been initialized yet.")

        return cls._instance

    @property
    def client(self) -> redis.asyncio.Redis:
        """
        Get the redis client after that it was initialized.

        This property will raise a `RedisSessionNotConnected` if it is accessed
        before the connect method is called.
        """
        if not self.valid:
            raise RedisSessionNotConnected(
                "attempting to access the client before the connection has been created."
            )
        return self._client

    @property
    def pool(self) -> redis.asyncio.ConnectionPool:
        """
        Get the connection pool after checking if it is still connected.

        This property is deprecated. Most operations should be performed on
        the client directly. Operations which benefit from managing the pool directly
        can access it from `client.connection_pool`.

        This property will raise a `RedisSessionNotConnected` if it's accessed after
        before the connect method is called.
        """
        # The validation and error logic is handled by the client property
        warnings.warn(
            DeprecationWarning(
                "pool property is deprecated. Most operations should be performed on "
                "the client directly. Operations which benefit from managing the pool directly "
                "can access it from client.connection_pool.",
            ),
            stacklevel=2,
        )
        return self.client.connection_pool

    async def connect(self, *, ping: bool = True) -> RedisSession:
        """
        Connect to Redis by instantiating the redis instance.

        If ping is True, a PING will be performed to ensure the connection is valid.
        If it's False, it'll be assumed the session is valid, and it's up to the user to
        manage when it's not.
        """
        log.debug("Creating Redis client.")

        # Decide if we want to use `fakeredis` or an actual Redis server. The
        # option to use `fakeredis.aioredis` is provided to aid with running the
        # application in a development environment and to aid with writing
        # unittests.
        if self._use_fakeredis:
            # Only import fakeredis when required. This ensures that it's not a
            # required dependency if someone's not planning on using it.
            try:
                import fakeredis.aioredis
            except ImportError:  # pragma: no cover
                raise FakeRedisNotInstalled(
                    "RedisSession was configured to use `fakeredis`, but it is not installed. "
                    "Either install `fakeredis` manually or install `async-rediscache` using "
                    "`pip install async-rediscache[fakeredis]` to enable support."
                )

            kwargs = dict(self._session_kwargs)
            # The following kwargs are not supported by fakeredis
            [kwargs.pop(kwarg, None) for kwarg in (
                "address", "username", "password", "port", "timeout"
            )]
            self._client = fakeredis.aioredis.FakeRedis(**kwargs)
        else:
            self._client = redis.asyncio.Redis(**self._session_kwargs)

        if ping:
            # Perform a PING to confirm the connection is valid
            await self._client.ping()

        self.valid = True
        return self
