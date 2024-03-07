# these tests require an externally running Postgres instance
# with the PGMQ Extension installed

from tembo_pgmq_python import Message, PGMQueue


def test_lifecycle() -> None:
    """tests complete lifecycle of the extension"""
    test_queue = "test_queue"
    test_message = {"hello": "world"}

    queue = PGMQueue(host="localhost", port=5432, username="postgres", password="postgres", database="postgres")
    queue.create_queue(test_queue)

    # start with empty queue
    purged = queue.purge(test_queue)
    assert purged >= 0

    # test sending/read
    msg_id = queue.send(test_queue, test_message)
    message: Message = queue.read(test_queue, vt = 20)
    assert message.message == test_message
    assert message.msg_id == msg_id, "read the wrong message"

    # test archive
    queue.archive(test_queue, message.msg_id)
    message = queue.read(test_queue, vt = 20)
    assert message is None, "no messaged expected in queue"

    # test delete
    msg_id = queue.send(test_queue, test_message)
    queue.delete(test_queue, msg_id)
    message = queue.read(test_queue, vt = 20)
    assert message is None, "no messaged expected in queue"

    # test purge
    msg_id = queue.send(test_queue, test_message)
    purged = queue.purge(test_queue)
    assert purged == 1
