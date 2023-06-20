from datetime import datetime

from tembo_pgmq_python import Message


def test_message() -> None:
    message = Message(
        msg_id=123,
        read_ct=1,
        enqueued_at=datetime.utcnow(),
        vt=datetime.utcnow(),
        message={"hello": "world"},
    )
    assert isinstance(message, Message)
