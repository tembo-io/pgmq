# these tests require an externally running Postgres instance
# with the PGMQ Extension installed

from tembo_pgmq_python import Message, PGMQueue


def test_lifecycle() -> None:
    """tests complete lifecycle of the extension"""
    test_queue = "test_queue"
    test_message = {"hello": "world"}

    queue = PGMQueue(host="localhost", port=5432, username="postgres", password="postgres", database="postgres")

    queue.create_queue(test_queue)

    msg_id = queue.send(test_queue, test_message)

    message: Message = queue.read(test_queue, vt = 20)

    assert message.message == test_message
    assert message.msg_id == msg_id
