# API

## `archive(queue_name text, msg_id bigint)`

Removes a single requested message from the specified queue and inserts it into the queue's archive.

Parameters
queue_name: The name of the queue that contains the message that needs to be archived.
msg_id: The ID of the message that needs to be archived.

Returns
Boolean value indicating success or failure of the operation.

Example; remove message with ID 1 from queue `my_queue` and archive it:

```sql
SELECT * FROM pgmq.archive('my_queue', 1);

 archive 
---------
       t
```

## `archive(queue_name text, msg_id bigint[])`

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

