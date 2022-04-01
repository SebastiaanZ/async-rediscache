import logging
from typing import Dict, ItemsView, Optional

from .base import RedisKeyType, RedisObject, RedisValueType

__all__ = [
    "RedisCache",
]

log = logging.getLogger(__name__)


class RedisCache(RedisObject):
    """
    A simplified interface for a Redis hash set.

    We implement several convenient methods that are fairly similar to have a
    dict behaves, and should be familiar to Python users. The biggest difference
    is that all the public methods in this class are coroutines, and must be
    awaited.

    Because of limitations in Redis, this cache will only accept strings and
    integers as keys and strings, integers, floats, and bools as values.

    By default, the namespace key of a RedisCache is automatically determined
    by the name of the owner class and the class attribute assigned to the
    RedisQueue instance. To bind a RedisQueue to a specific namespace, pass the
    namespace as the `namespace` keyword argument to constructor.

    Please note that for automatic namespacing, this class MUST be created as a
    class attribute to properly initialize the instance's namespace. See
    `__set_name__` for more information about how this works.

    Simple example for how to use this:

    class SomeCog(Cog):
        # To initialize a valid RedisCache, just add it as a class attribute
        # here. Do not add it to the __init__ method or anywhere else, it MUST
        # be a class attribute. Do not pass any parameters.
        cache = RedisCache()

        async def my_method(self):

            # Now we're ready to use the RedisCache.
            #
            # We can store some stuff in the cache just by doing this.
            # This data will persist through restarts!
            await self.cache.set("key", "value")

            # To get the data, simply do this.
            value = await self.cache.get("key")

            # Other methods work more or less like a dictionary.
            # Checking if something is in the cache
            await self.cache.contains("key")

            # iterating the cache
            for key, value in await self.cache.items():
                print(value)

            # We can even iterate in a comprehension!
            consumed = [value for key, value in await self.cache.items()]

    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the RedisCache."""
        super().__init__(*args, **kwargs)

    async def set(self, key: RedisKeyType, value: RedisValueType) -> None:
        """Store an item in the Redis cache."""
        # Convert to a typestring and then set it
        key = self._key_to_typestring(key)
        value = self._value_to_typestring(value)

        log.debug(f"Setting {key} to {value}.")
        await self.redis_session.client.hset(self.namespace, key, value)

    async def get(
            self, key: RedisKeyType, default: Optional[RedisValueType] = None
    ) -> Optional[RedisValueType]:
        """Get an item from the Redis cache."""
        key = self._key_to_typestring(key)

        log.debug(f"Attempting to retrieve {key}.")
        value = await self.redis_session.client.hget(self.namespace, key)
        return self._maybe_value_from_typestring(value, default)

    async def delete(self, key: RedisKeyType) -> None:
        """
        Delete an item from the Redis cache.

        If we try to delete a key that does not exist, it will simply be ignored.

        See https://redis.io/commands/hdel for more info on how this works.
        """
        key = self._key_to_typestring(key)

        log.debug(f"Attempting to delete {key}.")
        return await self.redis_session.client.hdel(self.namespace, key)

    async def contains(self, key: RedisKeyType) -> bool:
        """
        Check if a key exists in the Redis cache.

        Return True if the key exists, otherwise False.
        """
        key = self._key_to_typestring(key)
        exists = await self.redis_session.client.hexists(self.namespace, key)

        log.debug(f"Testing if {key} exists in the RedisCache - Result is {exists}")
        return exists

    async def items(self) -> ItemsView:
        """
        Fetch all the key/value pairs in the cache.

        Returns a normal ItemsView, like you would get from dict.items().

        Keep in mind that these items are just a _copy_ of the data in the
        RedisCache - any changes you make to them will not be reflected
        into the RedisCache itself. If you want to change these, you need
        to make a .set call.

        Example:
        items = await my_cache.items()
        for key, value in items:
            # Iterate like a normal dictionary
        """
        items = await self.redis_session.client.hgetall(self.namespace)
        items = self._dict_from_typestring(items).items()
        log.debug(f"Retrieving all key/value pairs from cache, total of {len(items)} items.")
        return items

    async def length(self) -> int:
        """Return the number of items in the Redis cache."""
        number_of_items = await self.redis_session.client.hlen(self.namespace)
        log.debug(f"Returning length. Result is {number_of_items}.")
        return number_of_items

    async def to_dict(self) -> Dict:
        """Convert to dict and return."""
        return {key: value for key, value in await self.items()}

    async def clear(self) -> None:
        """Deletes the entire hash from the Redis cache."""
        log.debug("Clearing the cache of all key/value pairs.")
        await self.redis_session.client.delete(self.namespace)

    async def pop(
            self, key: RedisKeyType, default: Optional[RedisValueType] = None
    ) -> RedisValueType:
        """Get the item, remove it from the cache, and provide a default if not found."""
        pop_script = await self._load_script("rediscache_pop.lua")
        key = self._key_to_typestring(key)

        log.debug(f"Popping {key!r} from the cache.")
        value = await self.redis_session.client.evalsha(pop_script, 2, self.namespace, key)

        return self._maybe_value_from_typestring(value, default)

    async def update(self, items: Dict[RedisKeyType, RedisValueType]) -> None:
        """
        Update the Redis cache with multiple values.

        This works exactly like dict.update from a normal dictionary. You pass
        a dictionary with one or more key/value pairs into this method. If the
        keys do not exist in the RedisCache, they are created. If they do exist,
        the values are updated with the new ones from `items`.

        Please note that keys and the values in the `items` dictionary
        must consist of valid RedisKeyTypes and RedisValueTypes.
        """
        log.debug(f"Updating the cache with the following items:\n{items}")
        await self.redis_session.client.hset(
            self.namespace,
            mapping=self._dict_to_typestring(items)
        )

    async def increment(self, key: RedisKeyType, amount: Optional[float] = 1) -> float:
        """
        Increment the value by `amount`.

        This works for both floats and ints, but will raise a TypeError
        if you try to do it for any other type of value.

        This also supports negative amounts, although it would provide better
        readability to use .decrement() for that.
        """
        log.debug(f"Attempting to increment/decrement the value with the key {key} by {amount}.")

        if type(amount) not in (int, float):
            raise TypeError("the increment amount must be an `int` or `float`.")

        increment_script = await self._load_script("rediscache_increment.lua")

        keys = [self.namespace, self._key_to_typestring(key)]
        args = [self._value_to_typestring(amount)]
        value = await self.redis_session.client.evalsha(increment_script, len(keys), *keys, *args)

        return self._maybe_value_from_typestring(value)

    async def decrement(self, key: RedisKeyType, amount: Optional[float] = 1) -> float:
        """
        Decrement the value by `amount`.

        Basically just does the opposite of .increment.
        """
        return await self.increment(key, -amount)
