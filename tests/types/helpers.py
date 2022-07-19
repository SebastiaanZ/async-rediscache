import unittest
import unittest.mock

import fakeredis.aioredis

from async_rediscache.session import RedisSession


class BaseRedisObjectTests(unittest.IsolatedAsyncioTestCase):
    """Base class for Redis data type test classes."""

    async def asyncSetUp(self):
        """Patch the RedisSession to pass in a fresh fakeredis client for each test."""
        self.patcher = unittest.mock.patch("async_rediscache.types.base.RedisSession")
        self.mock_session: RedisSession = self.patcher.start()
        self.mock_session.get_current_session.return_value = self.mock_session
        self.mock_session.client = fakeredis.aioredis.FakeRedis()  # noqa

        # Flush everything from the database to prevent carry-overs between tests
        await self.mock_session.client.flushall()

    async def asyncTearDown(self):
        self.patcher.stop()
