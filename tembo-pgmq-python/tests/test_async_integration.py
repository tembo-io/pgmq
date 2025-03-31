import unittest
import time
from tembo_pgmq_python.messages import Message
from tembo_pgmq_python.async_queue import PGMQueue
from tembo_pgmq_python.decorators import async_transaction as transaction
from datetime import datetime, timezone, timedelta

# Function to load environment variables


class BaseTestPGMQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Purge the queue before each test to ensure a clean state."""
        self.queue = PGMQueue(
            host="localhost",
            port="5432",
            username="postgres",
            password="postgres",
            database="postgres",
        )
        await self.queue.init()

        # Test database connection first
        try:
            pool = self.queue.pool
            async with pool.acquire() as conn:
                await conn.fetch("SELECT 1")
                print("Connection successful (without env)")
        except Exception as e:
            raise Exception(f"Database connection failed: {e}")

        self.test_queue = "test_queue"
        self.test_message = {"hello": "world"}
        await self.queue.create_queue(self.test_queue)
        await self.queue.purge(self.test_queue)

    async def asyncTearDown(self):
        await self.queue.pool.close()

    async def test_create_queue(self):
        """Test creating a queue."""
        await self.queue.create_queue("test_queue_2")
        msg_id = await self.queue.send("test_queue_2", self.test_message)
        self.assertIsInstance(msg_id, int)

    async def test_send_and_read_message(self):
        """Test sending and reading a message from the queue."""
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        message: Message = await self.queue.read(self.test_queue, vt=20)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    async def test_send_and_read_message_without_vt(self):
        """Test sending and reading a message from the queue without VT."""
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        message: Message = await self.queue.read(self.test_queue)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    async def test_send_message_with_delay(self):
        """Test sending a message with a delay."""
        msg_id = await self.queue.send(self.test_queue, self.test_message, delay=5)
        message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "Message should not be visible yet")
        time.sleep(5)
        message: Message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNotNone(message, "Message should be visible after delay")
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    async def test_send_message_with_tz(self):
        """Test sending a message with a timestamp delay."""
        timestamp = datetime.now(timezone.utc) + timedelta(seconds=5)
        msg_id = await self.queue.send(self.test_queue, self.test_message, tz=timestamp)
        message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "Message should not be visible yet")
        time.sleep(5)
        message: Message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNotNone(message, "Message should be visible after delay")
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    async def test_archive_message(self):
        """Test archiving a message in the queue."""
        _ = await self.queue.send(self.test_queue, self.test_message)
        message = await self.queue.read(self.test_queue, vt=20)
        await self.queue.archive(self.test_queue, message.msg_id)
        message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "No message expected in queue")

    async def test_delete_message(self):
        """Test deleting a message from the queue."""
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        await self.queue.delete(self.test_queue, msg_id)
        message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "No message expected in queue")

    async def test_send_batch(self):
        """Test sending a batch of messages to the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = await self.queue.send_batch(self.test_queue, messages)
        self.assertEqual(len(msg_ids), 2)

    async def test_read_batch(self):
        """Test reading a batch of messages from the queue."""
        messages = [self.test_message, self.test_message]
        await self.queue.send_batch(self.test_queue, messages)
        read_messages = await self.queue.read_batch(
            self.test_queue, vt=20, batch_size=2
        )
        self.assertEqual(len(read_messages), 2)
        for message in read_messages:
            self.assertEqual(message.message, self.test_message)
        no_message = await self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(no_message, "Messages should be invisible after read_batch")

    async def test_pop_message(self):
        """Test popping a message from the queue."""
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        message = await self.queue.pop(self.test_queue)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Popped the wrong message")

    async def test_purge_queue(self):
        """Test purging the queue."""
        messages = [self.test_message, self.test_message]
        await self.queue.send_batch(self.test_queue, messages)
        purged = await self.queue.purge(self.test_queue)
        self.assertEqual(purged, len(messages))

    async def test_metrics(self):
        """Test getting queue stats."""
        await self.queue.create_queue(self.test_queue)
        await self.queue.send(self.test_queue, self.test_message)
        stats = await self.queue.metrics(self.test_queue)

    async def test_metrics_all(self):
        """Test getting metrics for all queues."""
        await self.queue.create_queue(self.test_queue)
        await self.queue.send(self.test_queue, self.test_message)
        all_stats = await self.queue.metrics_all()
        self.assertGreaterEqual(len(all_stats), 1)
        for stats in all_stats:
            self.assertIsInstance(stats.queue_name, str)
            self.assertIsInstance(stats.queue_length, int)
            self.assertIsInstance(stats.total_messages, int)
            self.assertIsNotNone(stats.scrape_time)

    async def test_read_with_poll(self):
        """Test reading messages from the queue with polling."""
        await self.queue.send(self.test_queue, self.test_message)
        messages = await self.queue.read_with_poll(
            self.test_queue, vt=20, qty=1, max_poll_seconds=5, poll_interval_ms=100
        )
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message, self.test_message)

    async def test_archive_batch(self):
        """Test archiving multiple messages in the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = await self.queue.send_batch(self.test_queue, messages)
        await self.queue.archive_batch(self.test_queue, msg_ids)
        read_messages = await self.queue.read_batch(
            self.test_queue, vt=20, batch_size=2
        )
        self.assertEqual(len(read_messages), 0)

    async def test_delete_batch(self):
        """Test deleting multiple messages from the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = await self.queue.send_batch(self.test_queue, messages)
        await self.queue.delete_batch(self.test_queue, msg_ids)
        read_messages = await self.queue.read_batch(
            self.test_queue, vt=20, batch_size=2
        )
        self.assertEqual(len(read_messages), 0)

    async def test_set_vt(self):
        """Test setting the visibility timeout for a specific message."""
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        updated_message = await self.queue.set_vt(self.test_queue, msg_id, vt=60)
        self.assertEqual(
            updated_message.vt.second,
            (datetime.now(timezone.utc) + timedelta(seconds=60)).second,
        )

    async def test_list_queues(self):
        """Test listing all queues."""
        queues = await self.queue.list_queues()
        self.assertIn(self.test_queue, queues)

    async def test_detach_archive(self):
        """Test detaching an archive from a queue."""
        await self.queue.send(self.test_queue, self.test_message)
        await self.queue.archive(self.test_queue, 1)
        await self.queue.detach_archive(self.test_queue)
        # This is just a basic call to ensure the method works without exceptions.

    async def test_drop_queue(self):
        """Test dropping a queue."""
        await self.queue.create_queue("test_queue_to_drop")
        await self.queue.drop_queue("test_queue_to_drop")
        queues = await self.queue.list_queues()
        self.assertNotIn("test_queue_to_drop", queues)

    async def test_validate_queue_name(self):
        """Test validating the length of a queue name."""
        valid_queue_name = "a" * 47
        invalid_queue_name = "a" * 49
        # Valid queue name should not raise an exception
        await self.queue.validate_queue_name(valid_queue_name)
        # Invalid queue name should raise an exception
        with self.assertRaises(Exception) as context:
            await self.queue.validate_queue_name(invalid_queue_name)
        self.assertIn("queue name is too long", str(context.exception))

    async def test_transaction_create_queue(self):
        @transaction
        async def transactional_create_queue(queue):
            await queue.create_queue("test_queue_txn")
            raise Exception("Simulated failure")

        try:
            await transactional_create_queue(self.queue)
        except Exception:
            pass

        queues = await self.queue.list_queues()
        self.assertNotIn("test_queue_txn", queues)

    async def test_transaction_rollback(self):
        @transaction
        async def transactional_operation(queue):
            await queue.send(
                self.test_queue,
                self.test_message,
            )
            raise Exception("Intentional failure")

        try:
            await transactional_operation(self.queue)
        except Exception:
            pass

        message = await self.queue.read(self.test_queue)
        self.assertIsNone(message, "No message expected in queue after rollback")

    async def test_transaction_send_and_read_message(self):
        @transaction
        async def transactional_send(queue, conn):
            await queue.send(self.test_queue, self.test_message, conn=conn)

        await transactional_send(self.queue)

        message = await self.queue.read(self.test_queue)
        self.assertIsNotNone(message, "Expected message in queue")
        self.assertEqual(message.message, self.test_message)


class TestPGMQueueWithEnv(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """Set up a connection to the PGMQueue using environment variables and create a test queue."""

        self.queue = PGMQueue()
        await self.queue.init()

        # Test database connection first
        try:
            pool = self.queue.pool
            async with pool.acquire() as conn:
                await conn.fetch("SELECT 1")
                print("Connection successful (with env)")
        except Exception as e:
            raise Exception(f"Database connection failed: {e}")

        self.test_queue = "test_queue"
        self.test_message = {"hello": "world"}
        await self.queue.create_queue(self.test_queue)


if __name__ == "__main__":
    unittest.main()
