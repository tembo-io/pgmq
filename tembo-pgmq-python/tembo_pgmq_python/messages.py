from dataclasses import dataclass
from datetime import datetime


@dataclass
class Message:
    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict


@dataclass
class QueueMetrics:
    queue_name: str
    queue_length: int
    newest_msg_age_sec: int
    oldest_msg_age_sec: int
    total_messages: int
    scrape_time: datetime
