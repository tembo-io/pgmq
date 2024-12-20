from sqlalchemy import text
from sqlalchemy.orm import Session


def check_queue_exists(db_session: Session, queue_name: str) -> bool:
    row = db_session.execute(
        text(
            "SELECT queue_name FROM pgmq.list_queues() WHERE queue_name = :queue_name ;"
        ),
        {"queue_name": queue_name},
    ).first()
    return row is not None and row[0] == queue_name
