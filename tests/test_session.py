import unittest.mock

from async_rediscache import session


class RedisSessionTests(unittest.IsolatedAsyncioTestCase):
    """Tests for the RedisSession wrapper class."""

    def tearDown(self) -> None:
        """Explicitly remove `RedisSession`s after each test."""
        try:
            redis_session = session.RedisSession.get_current_session()
            redis_session.client.flushall()
        except (session.RedisSessionNotInitialized, session.RedisSessionNotConnected):
            pass
        session.RedisSession._instance = None

    async def test_singleton(self):
        """Test that the same session is returned from multiple constructions."""
        first = session.RedisSession("", use_fakeredis=True)
        second = session.RedisSession("", use_fakeredis=True)
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
            _ = session.RedisSession("").client

    async def test_fakeredis_get_args_from_url(self):
        """Test that sessions using fakeredis correctly parse arguments from the url."""
        # Using a URI with all arguments to ensure everything works as expected
        redis_session = session.RedisSession(
            "redis://username:password@localhost:6379/1?timeout=32s",
            use_fakeredis=True
        )
        await redis_session.connect()
        self.assertEqual(1, redis_session.client.connection_pool.connection_kwargs["db"])
