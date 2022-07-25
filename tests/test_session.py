import unittest.mock

from async_rediscache import session


class RedisSessionTests(unittest.IsolatedAsyncioTestCase):
    """Tests for the RedisSession wrapper class."""

    def setUp(self) -> None:
        """Explicitly remove `RedisSession`s after each test."""
        session.RedisSession._instance = None

    async def test_singleton(self):
        """Test that the same session is returned from multiple constructions."""
        first = session.RedisSession(use_fakeredis=True)
        second = session.RedisSession(use_fakeredis=True)
        self.assertIs(first, second, "Only one RedisSession should exist at runtime.")
        self.assertIs(
            first,
            session.RedisSession.get_current_session(),
            "The session returned by get_current_session does not match the initial session."
        )

    async def test_get_session_checks_initialized(self):
        """Ensure an error is raised if the session is accessed before it's been initialized."""
        with self.assertRaises(session.RedisSessionNotInitialized):
            session.RedisSession.get_current_session()

    async def test_error_if_not_connected(self):
        """Test that no operations can be performed until the connect method is called."""
        with self.assertRaises(session.RedisSessionNotConnected):
            _ = session.RedisSession().client

    async def test_pool_deprecation_warning(self):
        """Test that accessing the pool through the session outputs a warning."""
        redis_session = await session.RedisSession(use_fakeredis=True).connect()
        with self.assertWarns(DeprecationWarning):
            _ = redis_session.pool

    @staticmethod
    async def test_no_connect_operations():
        """Test that no operations are performed on connect if ping is False."""
        # We use a purposefully invalid real connection, which should fail
        # should any operations be attempted.
        await session.RedisSession(host="invalid").connect(ping=False)
