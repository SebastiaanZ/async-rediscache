import asyncio

from async_rediscache import types
from .helpers import BaseRedisObjectTests


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
