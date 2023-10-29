# Types

## message_record

The complete representation of a message in a queue.

| Attribute Name   | Type       | Description                |
| :---             |    :----   |                       :--- |
| msg_id           | bigint     | Unique ID of the message   |
| read_ct          | bigint     | Number of times the message has been read. Increments on read().   |
| enqueued_at           |  timestamp with time zone     | time that the message was inserted into the queue   |
| vt           | timestamp with time zone      | Timestamp when the message will become available for consumers to read   |
| message           | jsonb      | The message payload   |


Example:

```text
 msg_id | read_ct |          enqueued_at          |              vt               |      message       
--------+---------+-------------------------------+-------------------------------+--------------------
      1 |       1 | 2023-10-28 19:06:19.941509-05 | 2023-10-28 19:06:27.419392-05 | {"hello": "world"}
```