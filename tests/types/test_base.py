import unittest.mock

from async_rediscache import types


@unittest.mock.patch("async_rediscache.types.base.RedisSession")
class RedisObjectTests(unittest.IsolatedAsyncioTestCase):
    """Tests for the base RedisObject class."""

    def test_explicit_namespace_without_global_namespace(self, mock_session):
        """Test explicitly set namespace without a global namespace."""
        local_namespace = "van Rossum"

        mock_session.get_current_session().global_namespace = ""
        redis_object = types.RedisObject(namespace=local_namespace)
        self.assertEqual(redis_object.namespace, local_namespace)

    def test_explicit_namespace_with_global_namespace(self, mock_session):
        """Test explicitly set namespace with a global namespace."""
        global_namespace = "Guido"
        local_namespace = "van Rossum"
        expected_namespace = f"{global_namespace}.{local_namespace}"

        mock_session.get_current_session().global_namespace = global_namespace
        redis_object = types.RedisObject(namespace=local_namespace)
        self.assertEqual(redis_object.namespace, expected_namespace)

    def test_descriptor_based_namespace_without_global_namespace(self, mock_session):
        """Test namespace set by __set_name__ without a global namespace."""
        class Guido:
            van_rossum = types.RedisObject()

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Guido.van_rossum.namespace, "Guido.van_rossum")

    def test_descriptor_based_namespace_with_global_namespace(self, mock_session):
        """Test namespace set by __set_name__ with a global namespace."""
        class Guido:
            van_rossum = types.RedisObject()

        mock_session.get_current_session().global_namespace = "core_dev"
        self.assertEqual(Guido.van_rossum.namespace, "core_dev.Guido.van_rossum")

    def test_set_name_picks_first_attribute_for_namespace(self, mock_session):
        """Test that __set_name__ only sets the namespace for the first assignment."""
        class Kyle:
            stanley = types.RedisObject()
            broflovski = stanley

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Kyle.stanley.namespace, "Kyle.stanley")

    def test_set_name_does_not_override_explicit_namespace(self, mock_session):
        """Test that __set_name__ only sets the namespace for the first assignment."""
        class Kyle:
            stanley = types.RedisObject(namespace="python")

        mock_session.get_current_session().global_namespace = ""
        self.assertEqual(Kyle.stanley.namespace, "python")

    async def test_get_pool_connection_raises_without_set_namespace(self, _mock_session):
        """Test if `get_pool_connection` raises exception if a namespace wasn't set."""
        cache = types.RedisObject()

        with self.assertRaises(types.NoNamespaceError):
            await cache._get_pool_connection()

    def test_bypassing_global_namespace(self, mock_session):
        """Test if a RedisObject allows you to bypass the global namespace."""
        cache = types.RedisObject(namespace="Amsterdam", use_global_namespace=False)

        mock_session.get_current_session().global_namespace = "New"
        self.assertEqual(cache.namespace, "Amsterdam")

    @staticmethod
    def _get_pool_mock(mock_session):
        """Get a properly mocked pool property."""
        mock_session.get_current_session.return_value = mock_session
        pool_mock = unittest.mock.AsyncMock()
        type(mock_session).pool = property(fget=pool_mock)
        return pool_mock

    async def test_get_pool_connection_raises_exception_without_namespace(self, mock_session):
        """Test if ._get_pool_connection raises NoNamespaceError without namespace."""
        pool_mock = self._get_pool_mock(mock_session)
        cache = types.RedisObject()

        with self.assertRaises(types.NoNamespaceError):
            await cache._get_pool_connection()
        pool_mock.assert_not_awaited()

    async def test_get_pool_connection_gets_pool_connection(self, mock_session):
        """Test if ._get_pool_connection raises NoNamespaceError without namespace."""
        pool_mock = self._get_pool_mock(mock_session)
        cache = types.RedisObject(namespace="test")

        await cache._get_pool_connection()
        pool_mock.assert_awaited_once()

    def test_redis_session_property_gets_current_session(self, mock_session):
        """Test if the .redis_session property gets the current RedisSession."""
        cache = types.RedisObject(namespace="test")
        mock_session.get_current_session.return_value = unittest.mock.sentinel.redis_session
        self.assertEqual(cache.redis_session, unittest.mock.sentinel.redis_session)

    def test_typestring_conversion(self, _mock_session):
        """Test the typestring-related helper functions."""
        conversion_tests = (
            (12, "i|12"),
            (12.4, "f|12.4"),
            ("cowabunga", "s|cowabunga"),
        )

        cache = types.RedisObject(namespace="test")

        # Test conversion to typestring
        for _input, expected in conversion_tests:
            self.assertEqual(cache._value_to_typestring(_input), expected)

        # Test conversion from typestrings
        for _input, expected in conversion_tests:
            self.assertEqual(cache._value_from_typestring(expected), _input)

        # Test that exceptions are raised on invalid input
        with self.assertRaises(TypeError):
            cache._value_to_typestring(["internet"])

        with self.assertRaises(TypeError):
            cache._value_from_typestring("o|firedog")

    def test_dict_to_from_typestring(self, _mock_session):
        """Test if ._dict_to_typestring creates a properly typed dict."""
        cache = types.RedisObject(namespace="test")

        original = {
            "a": 1.1,
            "b": 200,
            3: "string value",
            4: True,
        }
        typestring_dictionary = {
            "s|a": "f|1.1",
            "s|b": "i|200",
            "i|3": "s|string value",
            "i|4": "b|1",
        }

        with self.subTest(method="_dict_to_typestring"):
            self.assertEqual(cache._dict_to_typestring(original), typestring_dictionary)

        with self.subTest(method="_dict_from_typestring"):
            self.assertEqual(cache._dict_from_typestring(typestring_dictionary), original)

    @unittest.mock.patch.object(types.base.asyncio, "Lock")
    async def test_atomic_transaction(self, mock_asyncio_lock, _mock_session):
        cache = types.RedisObject(namespace="test")

        # Set up proper mocking of `asyncio.Lock`
        async_lock_context_manager = unittest.mock.AsyncMock()
        mock_asyncio_lock.return_value = async_lock_context_manager

        # Create a mock method with a sentinel return value and decorate it
        mock_method = unittest.mock.AsyncMock(return_value=unittest.mock.sentinel.method_return)
        mock_method.__qualname__ = "mock_method"
        decorated_method = cache.atomic_transaction(mock_method)

        # Get the result by running the decorated method
        result = await decorated_method("one", kwarg="two")

        # Assert that the original method was awaited with the proper arguments
        mock_method.assert_awaited_once_with("one", kwarg="two")

        # Assert that the return value is properly propagated
        self.assertEqual(result, unittest.mock.sentinel.method_return)

        # Run decorated method another time to check that we only create lock once
        await decorated_method("one", kwarg="two")

        # Assert lock creation and acquisition
        mock_asyncio_lock.assert_called_once_with()

        # Assert that we've acquired and released the lock twice
        self.assertEqual(async_lock_context_manager.__aenter__.await_count, 2)
        self.assertEqual(async_lock_context_manager.__aexit__.await_count, 2)
