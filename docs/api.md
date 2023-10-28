# API

## Sending Messages

### send

```text
pgmq.send(
    queue_name text,
    msg jsonb,
    delay integer DEFAULT 0
)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg   | jsonb        | The message to send to the queue      |
| delay   | integer        | Time in seconds before the message becomes visible. Defaults to 0.      |

### send_batch

```text
pgmq.send_batch(
    queue_name text,
    msgs jsonb[],
    delay integer DEFAULT 0
)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msgs   | jsonb[]       | Array of messages to send to the queue      |
| delay   | integer        | Time in seconds before the messages becomes visible. Defaults to 0.      |

## Reading Messages

### read

```text
pgmq.read(
    queue_name text,
    vt integer,
    qty integer)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| vt   | integer       | Time in seconds that the message become invisible after reading.      |
| qty   | integer        | The number of messages to read from the queue. Defaults to 1.      |

 -> SETOF pgmq.message_record

### read_with_poll

```text
pgmq.read_with_poll(
    queue_name text,
    vt integer,
    qty integer,
    max_poll_seconds integer DEFAULT 5,
    poll_interval_ms integer DEFAULT 100
)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| vt   | integer       | Time in seconds that the message become invisible after reading.      |
| qty   | integer        | The number of messages to read from the queue. Defaults to 1.      |
| max_poll_seconds   | integer        | Time in seconds to wait for new messages to reach the queue. Defaults to 5.      |
| poll_interval_ms   | integer        | Milliseconds between the internal poll operations. Defaults to 100.      |

-> SETOF pgmq.message_record

### pop

```text
pgmq.pop(queue_name text)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

-> TABLE(msg_id bigint, msg text, created_at timestamp with time zone)

## Removing messages

### delete (single)

```text
delete (queue_name text, msg_id: bigint)
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_id      | bigint       | Message ID of the message to delete   |

-> TABLE

### delete (batch)

```text
delete (queue_name text, msg_id: bigint[])
```

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_id      | bigint[]       | Array of message IDs to delete   |

-> SETOF bigint

### purge_queue

```text
purge_queue(queue_name text)
```

Permanently deletes all messages in a queue.

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |

-> bigint

### archive (single)

```text
archive(queue_name text, msg_id bigint)
```

Removes a single requested message from the specified queue and inserts it into the queue's archive.

| Parameter      | Type | Description     |
| :---        |    :----   |          :--- |
| queue_name      | text       | The name of the queue   |
| msg_id      | bigint       | Message ID of the message to delete   |


Returns
Boolean value indicating success or failure of the operation.

Example; remove message with ID 1 from queue `my_queue` and archive it:

```sql
SELECT * FROM pgmq.archive('my_queue', 1);

 archive 
---------
       t
```

### archive (batch) 

`archive(queue_name text, msg_id bigint[])`

Removes a batch of requested messages from the specified queue and inserts them into the queue's archive.

Parameters
queue_name: The name of the queue that contains the message that needs to be archived.
msg_id: Array of message IDs that needs to be archived.

Returns
ARRAY of message ids that were successfully archived.

Example; remove messages with ID 1 and 2 from queue `my_queue` to the archive.

```sql
SELECT * FROM pgmq.archive('my_queue', ARRAY[1, 2]);

 archive 
---------
       1
       2
```

## Queue Management

### create

`create(queue_name text)`

### create_partitioned

`create_partitioned (queue-ue_name text, partition_interval text DEFAULT '10000'::text, retention_interval text DEFAULT '100000'::text )`

### create_unlogged

create_unlogged(queue_name text) -> void

### detach_archive

`detach_archive(queue_name text)`

### drop_queue

`drop_queue(queue_name text)`

 -> boolean



## Utilities

### set_vt

`set_vt(queue_name text, msg_id bigint, vt_offset integer)`
-> TABLE(msg_id bigint, read_ct integer, enqueued_at time
stamp with time zone, vt timestamp with time zone, message jsonb)


### list_queues

`list_queues()`

 -> TABLE(queue_name text, created_at timestamp with time zone, is_partitioned boolean, is_unlogged boolean) 

### metrics

`metrics(queue_name: text)`
 -> TABLE(queue_name text, queue_length bigint, newest_msg_age_sec integer, oldest_msg_age_sec integer, total_messages bigint, scrape_time timestamp with time zone)

### metrics_all

`metrics_all()`
 -> TABLE(queue_name text, queue_length bigint, newest_msg_age_sec integer, oldest_msg_age_sec integer, total_messages bigint, scrape_time timestamp with time zone)
