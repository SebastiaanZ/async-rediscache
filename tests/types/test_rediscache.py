import datetime

import time_machine

from async_rediscache import types
from .helpers import BaseRedisObjectTests


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

    async def test_increment_raises_type_error_for_invalid_types(self):
        """Test if `.increment` raises TypeError for invalid types."""
        test_cases = (
            {"initial": 100, "increment": "Python Discord"},
            {"initial": 1.1, "increment": True},
            {"initial": "Python Discord", "increment": 200},
            {"initial": True, "increment": 2.2},
        )

        for case in test_cases:
            await self.cache.set("value", case["initial"])
            with self.subTest(**case):
                with self.assertRaises(TypeError):
                    await self.cache.increment("value", amount=case["increment"])

    async def test_expiry_expires_after_timeout(self):
        """Test setting an expiry on a RedisCache."""
        with time_machine.travel(0, tick=False) as traveller:
            await self.cache.set("key", "value")
            result = await self.cache.set_expiry(10)
            self.assertTrue(result)
            traveller.shift(5)
            self.assertEqual(await self.cache.get("key"), "value")
            traveller.shift(6)
            self.assertIsNone(await self.cache.get("key"))

    async def test_expiry_returns_false_for_nonexisting_key(self):
        """The set_expiry method should return `False` for non-existing keys."""
        # Before settings the first key->value, the outer namespace key does
        # not exist yet.
        self.assertFalse(await self.cache.set_expiry(10))

    async def test_set_expiry_at_expires_after_timestamp(self):
        """The namespace should expire after the specified timestamp."""
        with time_machine.travel(1100, tick=False) as traveller:
            await self.cache.set("key", "value")
            result = await self.cache.set_expiry_at(1110)
            self.assertTrue(result)
            traveller.shift(5)
            self.assertEqual(await self.cache.get("key"), "value")
            traveller.shift(6)
            self.assertIsNone(await self.cache.get("key"))

    async def test_set_expiry_at_accepts_datetime(self):
        """The namespace should expire after the specified timestamp."""
        dt = datetime.datetime(2021, 1, 1, 12, 11, 10, tzinfo=datetime.timezone.utc)
        delta_expiry = datetime.timedelta(seconds=50_000_000)

        dt_expiry = dt + delta_expiry
        with time_machine.travel(dt, tick=False) as traveller:
            await self.cache.set("key", "value")
            result = await self.cache.set_expiry_at(dt_expiry)
            self.assertTrue(result)
            traveller.move_to(dt_expiry - datetime.timedelta(seconds=1))
            self.assertEqual(await self.cache.get("key"), "value")
            traveller.move_to(dt_expiry + datetime.timedelta(seconds=1))
            self.assertIsNone(await self.cache.get("key"))

    async def test_expiry_at_returns_false_for_nonexisting_key(self):
        """The set_expiry method should return `False` for non-existing keys."""
        # Before settings the first key->value, the outer namespace key does
        # not exist yet.
        self.assertFalse(await self.cache.set_expiry_at(10))
