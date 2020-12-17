import asyncio

from async_rediscache import types
from .helpers import BaseRedisObjectTests


class RedisQueueTests(BaseRedisObjectTests):
    """Tests for the RedisQueue datatype."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.queue = types.RedisQueue(namespace="test_queue")

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


class RedisTaskQueueTests(BaseRedisObjectTests):
    """Tests for teh RedisTaskQueue class."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.queue = types.RedisTaskQueue(namespace="task_queue")

    async def test_get_also_pushes_task_to_pending(self):
        """Test if the `.get` also pushes the task to the pending tasks queue."""
        test_value = "Python Discord"
        await self.queue.put(test_value)
        task = await self.queue.get()

        with await self.mock_session.pool as connection:
            queue_length = await connection.llen(self.queue.namespace)
            pending_length = await connection.llen(self.queue.namespace_pending)

        self.assertEqual(task.value, test_value)
        self.assertEqual(queue_length, 0)
        self.assertEqual(pending_length, 1)

    async def test_task_finalize_correctly_finalizes_task(self):
        """`task.finalize` should remove the task from the pending queue and marks it as done.`"""
        test_value = "Python Discord"
        await self.queue.put(test_value)
        self.assertEqual(await self.queue.qsize(), 1)

        task = await self.queue.get()
        self.assertEqual(await self.queue.qsize(), 0)
        self.assertEqual(task.value, test_value)

        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)

        self.assertEqual(pending_length, 1)

        await task.finalize()

        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)

        self.assertEqual(pending_length, 0)
        self.assertTrue(task.done)

    async def test_reschedule_task_puts_task_back_on_queue(self) -> None:
        """Rescheduling a task should put it back on the queue."""
        test_value = "Python Discord"
        await self.queue.put(test_value)
        self.assertEqual(await self.queue.qsize(), 1)

        task = await self.queue.get()
        self.assertEqual(await self.queue.qsize(), 0)
        self.assertEqual(task.value, test_value)

        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)
        self.assertEqual(pending_length, 1)

        await self.queue.reschedule_pending_task(task)

        self.assertEqual(await self.queue.qsize(), 1)
        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)
        self.assertEqual(pending_length, 0)

    async def test_reschedule_all_tasks_should_requeue_all_tasks(self) -> None:
        """Reschedule all tasks should reschedule all tasks in the original order."""
        task_values = [f"task-{ident}" for ident in range(10)]

        for task_value in task_values:
            await self.queue.put(task_value)

        self.assertEqual(await self.queue.qsize(), len(task_values))

        task_values_iter = iter(task_values)
        async for task in self.queue:
            self.assertEqual(next(task_values_iter), task.value)

        self.assertEqual(await self.queue.qsize(), 0)
        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)
        self.assertEqual(pending_length, 10)

        await self.queue.reschedule_all_pending_client_tasks()

        self.assertEqual(await self.queue.qsize(), 10)
        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)
        self.assertEqual(pending_length, 0)

    async def test_task_raises_TaskAlreadyDone_for_second_finalize(self) -> None:
        """Should raise TaskAlreadyDone for finalizing a task that is already done."""
        await self.queue.put("Hello")
        task = await self.queue.get()
        await task.finalize()

        with self.assertRaises(types.TaskAlreadyDone):
            await task.finalize()

    async def test_task_queue_raises_TaskNotPending_for_finalizing_nonpending_task(self) -> None:
        """Should raise TaskNotPending for finalizing a task not found in pending queue."""
        await self.queue.put("Hello")
        task = await self.queue.get()
        await task.reschedule()

        with self.assertRaises(types.TaskNotPending):
            await task.finalize()

    async def test_reschedule_task_by_value(self) -> None:
        """We should also be able to reschedule a task by its value."""
        value = "Some interesting value"
        await self.queue.put(value)
        self.assertEqual((await self.queue.get()).value, value)
        await self.queue.reschedule_pending_task(value)

        self.assertEqual(await self.queue.qsize(), 1)
        with await self.mock_session.pool as connection:
            pending_length = await connection.llen(self.queue.namespace_pending)
        self.assertEqual(pending_length, 0)

    async def test_rescheduling_unknown_task_raises_TaskNotPending(self) -> None:
        """Rescheduling an unknown task raises TaskNotPending."""
        with self.assertRaises(types.TaskNotPending):
            await self.queue.reschedule_pending_task("non-existent task")

    async def test_task_raises_RuntimeError_for_missing_owner_queue(self):
        """If the owner queue instance no longer exists, a task should raise a RuntimeError."""
        await self.queue.put("Hello")
        task = await self.queue.get()
        del self.queue
        with self.assertRaises(RuntimeError):
            _owner = task.owner  # noqa: F841


class RedisTaskQueueMultipleClientsTests(BaseRedisObjectTests):
    """Tests for teh RedisTaskQueue class."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.queue_one = types.RedisTaskQueue(namespace="task_queue", client_identifier="one")
        self.queue_two = types.RedisTaskQueue(namespace="task_queue", client_identifier="two")

    async def test_client_pending_queues_are_independent(self) -> None:
        """The pending queues of different clients should be independent."""
        values = [f"task-{i}" for i in range(6)]

        for value_one, value_two in zip(*[iter(values)]*2):
            await self.queue_one.put(value_one)
            await self.queue_two.put(value_two)

        # The central queue should be the same
        self.assertEqual(await self.queue_one.qsize(), len(values))
        self.assertEqual(await self.queue_one.qsize(), await self.queue_two.qsize())

        for value_one, value_two in zip(*[iter(values)]*2):
            self.assertEqual((await self.queue_one.get()).value, value_one)
            self.assertEqual((await self.queue_two.get()).value, value_two)

        # The central queue should now be depleted
        self.assertEqual(await self.queue_one.qsize(), 0)
        self.assertEqual(await self.queue_one.qsize(), await self.queue_two.qsize())

        with await self.mock_session.pool as connection:
            pending_one_length = await connection.llen(self.queue_one.namespace_pending)
            pending_two_length = await connection.llen(self.queue_two.namespace_pending)

        self.assertEqual(pending_one_length, len(values) // 2)
        self.assertEqual(pending_two_length, len(values) // 2)

        await self.queue_one.reschedule_all_pending_client_tasks()

        with await self.mock_session.pool as connection:
            pending_one_length = await connection.llen(self.queue_one.namespace_pending)
            pending_two_length = await connection.llen(self.queue_two.namespace_pending)

        self.assertEqual(pending_one_length, 0)
        self.assertEqual(pending_two_length, len(values) // 2)

        # Check if the values of queue_one have made it back on the main queue
        self.assertEqual(await self.queue_one.qsize(), len(values) // 2)

        await self.queue_two.reschedule_all_pending_client_tasks()

        with await self.mock_session.pool as connection:
            pending_one_length = await connection.llen(self.queue_one.namespace_pending)
            pending_two_length = await connection.llen(self.queue_two.namespace_pending)

        self.assertEqual(pending_one_length, 0)
        self.assertEqual(pending_two_length, 0)

        # Check if the values of queue_two have made it back on the main queue
        self.assertEqual(await self.queue_one.qsize(), len(values))
