import unittest
import unittest.mock

import fakeredis.aioredis


class BaseRedisObjectTests(unittest.IsolatedAsyncioTestCase):
    """Base class for Redis data type test classes."""

    async def asyncSetUp(self):
        """Patch the RedisSession to pass in a fresh fakeredis pool for each test."""
        self.patcher = unittest.mock.patch("async_rediscache.types.base.RedisSession")
        self.mock_session = self.patcher.start()
        self.mock_session.get_current_session.return_value = self.mock_session
        self.mock_session.pool = await fakeredis.aioredis.create_redis_pool()

    async def asyncTearDown(self):
        self.patcher.stop()
