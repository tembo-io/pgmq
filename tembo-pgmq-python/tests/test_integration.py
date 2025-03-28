import unittest
import time
from tembo_pgmq_python import Message, PGMQueue, transaction

from datetime import datetime, timezone, timedelta


class BaseTestPGMQueue(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a connection to the PGMQueue without using environment variables and create a test queue."""
        cls.queue = PGMQueue(
            host="localhost",
            port="5432",
            username="postgres",
            password="postgres",
            database="postgres",
            verbose=False,
        )

        # Test database connection first
        try:
            pool = cls.queue.pool
            with pool.connection() as conn:
                conn.execute("SELECT 1")
                print("Connection successful (without env)")
        except Exception as e:
            raise Exception(f"Database connection failed: {e}")

        cls.test_queue = "test_queue"
        cls.test_message = {"hello": "world"}
        cls.queue.create_queue(cls.test_queue)

    def setUp(self):
        """Purge the queue before each test to ensure a clean state."""
        self.queue.purge(self.test_queue)

    def test_create_queue(self):
        """Test creating a queue."""
        self.queue.create_queue("test_queue_2")
        msg_id = self.queue.send("test_queue_2", self.test_message)
        self.assertIsInstance(msg_id, int)

    def test_send_and_read_message(self):
        """Test sending and reading a message from the queue."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        message: Message = self.queue.read(self.test_queue, vt=20)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    def test_send_and_read_message_without_vt(self):
        """Test sending and reading a message from the queue without VT."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        message: Message = self.queue.read(self.test_queue)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    def test_send_message_with_delay(self):
        """Test sending a message with a delay."""
        msg_id = self.queue.send(self.test_queue, self.test_message, delay=5)
        message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "Message should not be visible yet")
        time.sleep(5)
        message: Message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNotNone(message, "Message should be visible after delay")
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    def test_send_message_with_tz(self):
        """Test sending a message with a timestamp delay."""
        timestamp = datetime.now(timezone.utc) + timedelta(seconds=5)
        msg_id = self.queue.send(self.test_queue, self.test_message, tz=timestamp)
        message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "Message should not be visible yet")
        time.sleep(5)
        message: Message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNotNone(message, "Message should be visible after timestamp delay")
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Read the wrong message")

    def test_archive_message(self):
        """Test archiving a message in the queue."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        message = self.queue.read(self.test_queue, vt=20)
        self.queue.archive(self.test_queue, message.msg_id)
        message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "No message expected in queue")

    def test_delete_message(self):
        """Test deleting a message from the queue."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        self.queue.delete(self.test_queue, msg_id)
        message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(message, "No message expected in queue")

    def test_send_batch(self):
        """Test sending a batch of messages to the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = self.queue.send_batch(self.test_queue, messages)
        self.assertEqual(len(msg_ids), 2)

    def test_read_batch(self):
        """Test reading a batch of messages from the queue."""
        messages = [self.test_message, self.test_message]
        self.queue.send_batch(self.test_queue, messages)
        read_messages = self.queue.read_batch(self.test_queue, vt=20, batch_size=2)
        self.assertEqual(len(read_messages), 2)
        for message in read_messages:
            self.assertEqual(message.message, self.test_message)
        no_message = self.queue.read(self.test_queue, vt=20)
        self.assertIsNone(no_message, "Messages should be invisible after read_batch")

    def test_pop_message(self):
        """Test popping a message from the queue."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        message = self.queue.pop(self.test_queue)
        self.assertEqual(message.message, self.test_message)
        self.assertEqual(message.msg_id, msg_id, "Popped the wrong message")

    def test_purge_queue(self):
        """Test purging the queue."""
        messages = [self.test_message, self.test_message]
        self.queue.send_batch(self.test_queue, messages)
        purged = self.queue.purge(self.test_queue)
        self.assertEqual(purged, len(messages))

    def test_metrics(self):
        """Test getting queue stats."""
        self.queue.create_queue(self.test_queue)
        self.queue.send(self.test_queue, self.test_message)
        stats = self.queue.metrics(self.test_queue)

    def test_metrics_all(self):
        """Test getting metrics for all queues."""
        self.queue.create_queue(self.test_queue)
        self.queue.send(self.test_queue, self.test_message)
        all_stats = self.queue.metrics_all()
        self.assertGreaterEqual(len(all_stats), 1)
        for stats in all_stats:
            self.assertIsInstance(stats.queue_name, str)
            self.assertIsInstance(stats.queue_length, int)
            self.assertIsInstance(stats.total_messages, int)
            self.assertIsNotNone(stats.scrape_time)

    def test_read_with_poll(self):
        """Test reading messages from the queue with polling."""
        self.queue.send(self.test_queue, self.test_message)
        messages = self.queue.read_with_poll(
            self.test_queue, vt=20, qty=1, max_poll_seconds=5, poll_interval_ms=100
        )
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message, self.test_message)

    def test_archive_batch(self):
        """Test archiving multiple messages in the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = self.queue.send_batch(self.test_queue, messages)
        self.queue.archive_batch(self.test_queue, msg_ids)
        read_messages = self.queue.read_batch(self.test_queue, vt=20, batch_size=2)
        self.assertEqual(len(read_messages), 0)

    def test_delete_batch(self):
        """Test deleting multiple messages from the queue."""
        messages = [self.test_message, self.test_message]
        msg_ids = self.queue.send_batch(self.test_queue, messages)
        self.queue.delete_batch(self.test_queue, msg_ids)
        read_messages = self.queue.read_batch(self.test_queue, vt=20, batch_size=2)
        self.assertEqual(len(read_messages), 0)

    def test_set_vt(self):
        """Test setting the visibility timeout for a specific message."""
        msg_id = self.queue.send(self.test_queue, self.test_message)
        updated_message = self.queue.set_vt(self.test_queue, msg_id, vt=60)
        self.assertEqual(
            updated_message.vt.second,
            (datetime.now(timezone.utc) + timedelta(seconds=60)).second,
        )

    def test_list_queues(self):
        """Test listing all queues."""
        queues = self.queue.list_queues()
        self.assertIn(self.test_queue, queues)

    def test_detach_archive(self):
        """Test detaching an archive from a queue."""
        self.queue.send(self.test_queue, self.test_message)
        self.queue.archive(self.test_queue, 1)
        self.queue.detach_archive(self.test_queue)

    def test_drop_queue(self):
        """Test dropping a queue."""
        self.queue.create_queue("test_queue_to_drop")
        self.queue.drop_queue("test_queue_to_drop")
        queues = self.queue.list_queues()
        self.assertNotIn("test_queue_to_drop", queues)

    def test_validate_queue_name(self):
        """Test validating the length of a queue name."""
        valid_queue_name = "a" * 47
        invalid_queue_name = "a" * 49
        # Valid queue name should not raise an exception
        self.queue.validate_queue_name(valid_queue_name)
        # Invalid queue name should raise an exception
        with self.assertRaises(Exception) as context:
            self.queue.validate_queue_name(invalid_queue_name)
        self.assertIn("queue name is too long", str(context.exception))

    def test_transaction_create_queue(self):
        """Test creating a queue within a transaction."""

        @transaction
        def transactional_create_queue(queue, conn=None):
            queue.create_queue("test_queue_txn", conn=conn)
            raise Exception("Intentional failure")

        try:
            transactional_create_queue(self.queue)
        except Exception:
            pass
        finally:
            queues = self.queue.list_queues()
            self.assertNotIn("test_queue_txn", queues)

    def test_transaction_send_and_read_message(self):
        """Test sending and reading a message within a transaction."""

        @transaction
        def transactional_send(queue, conn=None):
            queue.send(self.test_queue, self.test_message, conn=conn)
            raise Exception("Intentional failure")

        try:
            transactional_send(self.queue)
        except Exception:
            pass
        finally:
            message = self.queue.read(self.test_queue)
            self.assertIsNone(message, "No message expected in queue")

    def test_transaction_purge_queue(self):
        """Test purging a queue within a transaction."""

        self.queue.send(self.test_queue, self.test_message)

        @transaction
        def transactional_purge(queue, conn=None):
            queue.purge(self.test_queue, conn=conn)
            raise Exception("Intentional failure")

        try:
            transactional_purge(self.queue)
        except Exception:
            pass
        finally:
            message = self.queue.read(self.test_queue)
            self.assertIsNotNone(message, "Message expected in queue")

    def test_transaction_rollback(self):
        """Test rollback of a transaction."""

        @transaction
        def transactional_operation(queue, conn=None):
            queue.send(self.test_queue, self.test_message, conn=conn)
            raise Exception("Intentional failure to trigger rollback")

        try:
            transactional_operation(self.queue)
        except Exception:
            pass
        finally:
            message = self.queue.read(self.test_queue)
            self.assertIsNone(message, "No message expected in queue after rollback")


class TestPGMQueueWithEnv(BaseTestPGMQueue):
    @classmethod
    def setUpClass(cls):
        """Set up a connection to the PGMQueue using environment variables and create a test queue."""

        cls.queue = PGMQueue()

        # Test database connection first
        try:
            pool = cls.queue.pool
            with pool.connection() as conn:
                conn.execute("SELECT 1")
                print("Connection successful (with env)")
        except Exception as e:
            raise Exception(f"Database connection failed: {e}")

        cls.test_queue = "test_queue"
        cls.test_message = {"hello": "world"}
        cls.queue.create_queue(cls.test_queue)


if __name__ == "__main__":
    unittest.main()
