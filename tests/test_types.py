import asyncio
import unittest.mock

import fakeredis.aioredis

from async_rediscache import types


@unittest.mock.patch("async_rediscache.types.RedisSession")
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

    @unittest.mock.patch.object(types.asyncio, "Lock")
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


class BaseRedisObjectTests(unittest.IsolatedAsyncioTestCase):
    """Base class for Redis data type test classes."""

    async def asyncSetUp(self):
        """Patch the RedisSession to pass in a fresh fakeredis pool for each test."""
        self.patcher = unittest.mock.patch("async_rediscache.types.RedisSession")
        self.mock_session = self.patcher.start()
        self.mock_session.get_current_session.return_value = self.mock_session
        self.mock_session.pool = await fakeredis.aioredis.create_redis_pool()

    async def asyncTearDown(self):
        self.patcher.stop()


class RedisCacheTests(BaseRedisObjectTests):
    """Tests for the RedisCache data type."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.cache = types.RedisCache(namespace="test_cache")

    async def test_set_get_item(self):
        """Test that users can set and get items from the RedisDict."""
        test_cases = (
            ('favorite_fruit', 'melon'),
            ('favorite_number', 86),
            ('favorite_fraction', 86.54),
            ('favorite_boolean', False),
            ('other_boolean', True),
        )

        # Test that we can get and set different types.
        for key, value in test_cases:
            with self.subTest(key=key, value=value):
                await self.cache.set(key, value)
                returned_value = await self.cache.get(key)

                # A round trip should preserve the value
                self.assertEqual(returned_value, value)
                # A round trip should preserve the exact type of the value
                self.assertIs(type(returned_value), type(value))

    async def test_get_item_default_value(self):
        """Test if the get method returns the default value if a key was not found."""
        test_cases = ("a", 10, 1.1, True)

        for default_value in test_cases:
            with self.subTest(default_value=default_value):
                returned_value = await self.cache.get("non-existing key", default_value)

                # We should return the same object
                self.assertIs(returned_value, default_value)

    async def test_set_item_type(self):
        """Test that .set rejects keys and values that are not permitted."""
        test_cases = (["lemon", "melon", "apple"], 4.21, True)

        for invalid_key in test_cases:
            with self.subTest(invalid_key=invalid_key, invalid_key_type=type(invalid_key)):
                with self.assertRaises(TypeError):
                    await self.cache.set(invalid_key, "nice")

    async def test_delete_item(self):
        """Test that .delete allows us to delete stuff from the RedisCache."""
        # Add an item and verify that it gets added
        await self.cache.set("internet", "firetruck")
        self.assertEqual(await self.cache.get("internet"), "firetruck")

        # Delete that item and verify that it gets deleted
        await self.cache.delete("internet")
        self.assertIs(await self.cache.get("internet"), None)

    async def test_contains(self):
        """Test that we can check membership with .contains."""
        await self.cache.set('favorite_country', "Burkina Faso")

        self.assertIs(await self.cache.contains('favorite_country'), True)
        self.assertIs(await self.cache.contains('favorite_dentist'), False)

    async def test_items(self):
        """Test that the RedisDict can be iterated."""
        # Set up our test cases in the Redis cache
        test_cases = [
            ('favorite_turtle', 'Donatello'),
            ('second_favorite_turtle', 'Leonardo'),
            ('third_favorite_turtle', 'Raphael'),
        ]
        for key, value in test_cases:
            await self.cache.set(key, value)

        # Consume the AsyncIterator into a regular list, easier to compare that way.
        redis_items = [item for item in await self.cache.items()]

        # These sequences are probably in the same order now, but probably
        # isn't good enough for tests. Let's not rely on .hgetall always
        # returning things in sequence, and just sort both lists to be safe.
        redis_items = sorted(redis_items)
        test_cases = sorted(test_cases)

        # If these are equal now, everything works fine.
        self.assertSequenceEqual(test_cases, redis_items)

    async def test_length(self):
        """Test that we can get the correct .length from the RedisDict."""
        await self.cache.set('one', 1)
        await self.cache.set('two', 2)
        await self.cache.set('three', 3)
        self.assertEqual(await self.cache.length(), 3)

        await self.cache.set('four', 4)
        self.assertEqual(await self.cache.length(), 4)

    async def test_to_dict(self):
        """Test that the .to_dict method returns a workable dictionary copy."""
        test_data = [
            ('favorite_turtle', 'Donatello'),
            ('second_favorite_turtle', 'Leonardo'),
            ('third_favorite_turtle', 'Raphael'),
        ]
        for key, value in test_data:
            await self.cache.set(key, value)

        copy = await self.cache.to_dict()
        local_copy = {key: value for key, value in await self.cache.items()}
        self.assertIs(type(copy), dict)
        self.assertDictEqual(copy, local_copy)

    async def test_clear(self):
        """Test that the .clear method removes the entire hash."""
        await self.cache.set('teddy', 'with me')
        await self.cache.set('in my dreams', 'you have a weird hat')
        self.assertEqual(await self.cache.length(), 2)

        await self.cache.clear()
        self.assertEqual(await self.cache.length(), 0)

    async def test_pop(self):
        """Test that we can .pop an item from the RedisDict."""
        await self.cache.set('john', 'was afraid')

        self.assertEqual(await self.cache.pop('john'), 'was afraid')
        self.assertEqual(await self.cache.pop('pete', 'breakneck'), 'breakneck')
        self.assertEqual(await self.cache.length(), 0)

    async def test_update(self):
        """Test that we can .update the RedisDict with multiple items."""
        await self.cache.set("reckfried", "lona")
        await self.cache.set("bel air", "prince")
        await self.cache.update({
            "reckfried": "jona",
            "mega": "hungry, though",
        })

        result = {
            "reckfried": "jona",
            "bel air": "prince",
            "mega": "hungry, though",
        }
        self.assertDictEqual(await self.cache.to_dict(), result)

    async def test_increment(self):
        """Test the .increment and .decrement methods on float and integer values."""
        local_copy = {
            "int_value": 10,
            "float_value": 12.5,
        }

        await self.cache.set("int_value", local_copy["int_value"])
        await self.cache.set("float_value", local_copy["float_value"])

        for increment in ((), (25,), (25.5,), (-30,), (-35.5,)):
            for target in ("int_value", "float_value"):
                # Sanity check
                pre_increment = await self.cache.get(target)
                self.assertEqual(local_copy[target], pre_increment)

                with self.subTest(target=target, initial_value=pre_increment, increment=increment):
                    # unpack the value for our local copy
                    value = increment[0] if increment else 1

                    # first we increment
                    local_copy[target] += value
                    await self.cache.increment(target, *increment)
                    post_increment = await self.cache.get(target)
                    self.assertEqual(local_copy[target], post_increment)

                    # then, we decrement
                    local_copy[target] -= value
                    await self.cache.decrement(target, *increment)
                    post_decrement = await self.cache.get(target)
                    self.assertEqual(local_copy[target], post_decrement)


class RedisQueueTests(BaseRedisObjectTests):
    """Tests for the RedisQueue datatype."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.queue = types.RedisQueue(namespace="test_cache")

    async def test_put_get_no_wait(self):
        """Test the .put and .get method without waiting, open connections."""
        values_in = ("hello", 100, 1.1, True)

        for value_in in values_in:
            with self.subTest("single value put/get test", value=repr(value_in)):
                self.assertTrue(await self.queue.empty())

                await self.queue.put(value_in)
                value_out = await self.queue.get(wait=False)
                self.assertEqual(value_in, value_out)

        with self.subTest("RedisQueue works on FIFO principle"):
            self.assertTrue(await self.queue.empty())
            for value_in in values_in:
                await self.queue.put(value_in)

            values_out = [await self.queue.get(wait=False) for _ in range(len(values_in))]
            self.assertSequenceEqual(values_in, values_out)

        with self.subTest("complex sequence FIFO test"):
            self.assertTrue(await self.queue.empty())

            values_in = ("one", "two", "three", "four", "five")
            iter_values = iter(values_in)
            values_out = []

            # Five items, five items out, intermixed
            await self.queue.put(next(iter_values))
            await self.queue.put(next(iter_values))
            values_out.append(await self.queue.get(wait=False))
            await self.queue.put(next(iter_values))
            values_out.append(await self.queue.get(wait=False))
            await self.queue.put(next(iter_values))
            values_out.append(await self.queue.get(wait=False))
            values_out.append(await self.queue.get(wait=False))
            await self.queue.put(next(iter_values))
            values_out.append(await self.queue.get(wait=False))

            self.assertSequenceEqual(values_in, values_out)

        with self.subTest("test get_nowait partial method"):
            self.assertTrue(await self.queue.empty())

            value_in = "test"
            await self.queue.put_nowait(value_in)
            value_out = await self.queue.get_nowait()

            self.assertEqual(value_in, value_out)

    async def test_get_wait(self):
        """Test if get is able to wait for an item to become available."""
        with self.subTest("get with no timeout"):
            pending_get = asyncio.create_task(self.queue.get())
            self.assertFalse(pending_get.done())

            value_in = "spam"
            await self.queue.put(value_in)

            value_out = await pending_get
            self.assertEqual(value_in, value_out)
            self.assertTrue(pending_get.done())

        with self.subTest("get with timeout"):
            pending_get = asyncio.create_task(self.queue.get(timeout=1))
            value_out = await pending_get
            self.assertIsNone(value_out)

    async def test_qzise_and_empty(self):
        """Test if .qsize and .empty correctly reflect the size of the queue."""
        self.assertEqual(await self.queue.qsize(), 0)
        self.assertTrue(await self.queue.empty())

        await self.queue.put("one")
        await self.queue.put("two")
        self.assertEqual(await self.queue.qsize(), 2)
        self.assertFalse(await self.queue.empty())

        await self.queue.get()
        self.assertEqual(await self.queue.qsize(), 1)
        self.assertFalse(await self.queue.empty())

        await self.queue.get()
        self.assertEqual(await self.queue.qsize(), 0)
        self.assertTrue(await self.queue.empty())
