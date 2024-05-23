import unittest
import time
from tembo_pgmq_python import Message, PGMQueue


# Function to load environment variables


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
