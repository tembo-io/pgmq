-- CREATE pgmq.
CREATE EXTENSION IF NOT EXISTS pgmq;
CREATE EXTENSION IF NOT EXISTS pg_partman;

-- test_unlogged
-- CREATE with default retention and partition strategy
SELECT pgmq.create_unlogged('test_unlogged_queue');
SELECT * from pgmq.send('test_unlogged_queue', '{"hello": "world"}');
SELECT msg_id, read_ct, enqueued_at > NOW(), vt > NOW(), message
  FROM pgmq.read('test_unlogged_queue', 2, 1);

-- test_max_queue_name_size
-- CREATE with default retention and partition strategy
SELECT pgmq.create(repeat('a', 48));
SELECT pgmq.create(repeat('a', 47));

-- test_lifecycle
-- CREATE with default retention and partition strategy
SELECT pgmq.create('test_default_queue');

-- creating a queue must be idempotent
-- create with same name again, must be no error
SELECT pgmq.create('test_default_queue');

SELECT * from pgmq.send('test_default_queue', '{"hello": "world"}');

-- read message
-- vt=2, limit=1
\set msg_id 1
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1);

-- read message using conditional
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1, '{"hello": "world"}');

-- set VT to 5 seconds
SELECT vt > clock_timestamp() + '4 seconds'::interval
  FROM pgmq.set_vt('test_default_queue', :msg_id, 5);

-- read again, assert no messages because we just set VT to the future
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1);

-- read again, now using poll to block until message is ready
SELECT msg_id = :msg_id FROM pgmq.read_with_poll('test_default_queue', 10, 1, 10);

-- set VT to 5 seconds again for another read_with_poll test
SELECT vt > clock_timestamp() + '4 seconds'::interval
  FROM pgmq.set_vt('test_default_queue', :msg_id, 5);

-- read again, now using poll to block until message is ready
SELECT msg_id = :msg_id FROM pgmq.read_with_poll('test_default_queue', 10, 1, 10, 100, '{"hello": "world"}');

-- after reading it, set VT to now
SELECT msg_id = :msg_id FROM pgmq.set_vt('test_default_queue', :msg_id, 0);

-- read again, should have msg_id 1 again
SELECT msg_id = :msg_id FROM pgmq.read('test_default_queue', 2, 1);

-- send a batch of 2 messages
SELECT pgmq.create('batch_queue');
SELECT ARRAY( SELECT pgmq.send_batch(
    'batch_queue',
    ARRAY['{"hello": "world_0"}', '{"hello": "world_1"}']::jsonb[]
)) = ARRAY[1, 2]::BIGINT[];

-- CREATE with 5 seconds per partition, 10 seconds retention
SELECT pgmq.create_partitioned('test_duration_queue', '5 seconds', '10 seconds');

-- CREATE with 10 messages per partition, 20 messages retention
SELECT pgmq.create_partitioned('test_numeric_queue', '10 seconds', '20 seconds');

-- get metrics
SELECT queue_name, queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages
 FROM pgmq.metrics('test_duration_queue');

-- get metrics all
SELECT * from {PGMQ_SCHEMA}.metrics_all();

-- delete all the queues
-- delete partitioned queues
SELECT pgmq.drop_queue(queue, true)
  FROM unnest('{test_numeric_queue}'::text[]) AS queue;

-- test automatic partitioned status checking
SELECT pgmq.drop_queue(queue)
  FROM unnest('{test_duration_queue}'::text[]) AS queue;

-- drop the rest of the queues
SELECT pgmq.drop_queue(q.queue_name, true)
  FROM (SELECT queue_name FROM pgmq.list_queues()) AS q;

SELECT queue_name FROM pgmq.list_queues();

-- test_archive
SELECT pgmq.create('archive_queue');

-- no messages in the queue
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;

-- no messages in queue archive
SELECT COUNT(*) = 0 FROM pgmq.a_archive_queue;

-- put messages on the queue
\set msg_id1 1::bigint
\set msg_id2 2::bigint
SELECT send = :msg_id1 FROM pgmq.send('archive_queue', '0');
SELECT send = :msg_id2 FROM pgmq.send('archive_queue', '0');

-- two messages in the queue
SELECT COUNT(*) = 2 FROM pgmq.q_archive_queue;

-- archive the message. The first two exist so the id should be returned, the
-- last one doesn't
SELECT ARRAY(
    SELECT * FROM pgmq.archive('archive_queue', ARRAY[:msg_id1, :msg_id2])
) = ARRAY[:msg_id1, :msg_id2];

-- should be no messages left on the queue table
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;

-- should be two messages in archive
SELECT COUNT(*) = 2 FROM pgmq.a_archive_queue;

\set msg_id3 3::bigint
SELECT send = :msg_id3 FROM pgmq.send('archive_queue', '0');
SELECT COUNT(*) = 1 FROM pgmq.q_archive_queue;
SELECT * FROM pgmq.archive('archive_queue', :msg_id3);
SELECT COUNT(*) = 0 FROM pgmq.q_archive_queue;
SELECT COUNT(*) = 3 FROM pgmq.a_archive_queue;

-- test_read_read_with_poll
-- Creating queue
SELECT pgmq.create('test_read_queue');

-- Sending 3 messages to the queue
SELECT send = :msg_id1 FROM pgmq.send('test_read_queue', '0');
SELECT send = :msg_id2 FROM pgmq.send('test_read_queue', '0');
SELECT send = :msg_id3 FROM pgmq.send('test_read_queue', '0');

-- Reading with limit respects the limit
SELECT msg_id = :msg_id1 FROM pgmq.read('test_read_queue', 5, 1);

-- Reading respects the VT
SELECT ARRAY(
    SELECT msg_id FROM pgmq.read('test_read_queue', 10, 5)
) = ARRAY[:msg_id2, :msg_id3];

-- Read with poll will poll until the first message is available
SELECT clock_timestamp() AS start \gset
SELECT msg_id = :msg_id1 FROM pgmq.read_with_poll('test_read_queue', 10, 5, 5, 100);
SELECT clock_timestamp() - :'start' > '3 second'::interval;

-- test_purge_queue
SELECT pgmq.create('test_purge_queue');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');
SELECT * from pgmq.send('test_purge_queue', '0');

SELECT * FROM pgmq.purge_queue('test_purge_queue');
SELECT COUNT(*) = 0 FROM pgmq.q_test_purge_queue;

-- test_pop
SELECT pgmq.create('test_pop_queue');
SELECT * FROM pgmq.pop('test_pop_queue');

SELECT send AS first_msg_id from pgmq.send('test_pop_queue', '0') \gset
SELECT * from pgmq.send('test_pop_queue', '0');
SELECT * from pgmq.send('test_pop_queue', '0');

SELECT msg_id = :first_msg_id FROM pgmq.pop('test_pop_queue');

-- test_set_vt
SELECT pgmq.create('test_set_vt_queue');
SELECT * FROM pgmq.set_vt('test_set_vt_queue', 9999, 0);

SELECT send AS first_msg_id from pgmq.send('test_set_vt_queue', '0') \gset

-- set message invisible for 100 seconds
SELECT msg_id FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, 100);

-- read message, it should not be visible
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- make it visible
SELECT msg_id FROM pgmq.set_vt('test_set_vt_queue', :first_msg_id, 0);

-- set vt works if message is readable
SELECT msg_id from pgmq.read('test_set_vt_queue', 1, 1);

-- test_partitioned_delete
\set partition_interval 2
\set retention_interval 2

-- We first will drop pg_partman and assert that create fails without the
-- extension installed
DROP EXTENSION pg_partman;

SELECT * FROM pgmq.create_partitioned(
    'test_partitioned_queue',
    :'partition_interval',
    :'retention_interval'
);

-- With the extension existing, the queue is created successfully
CREATE EXTENSION pg_partman;
SELECT * FROM pgmq.create_partitioned(
    'test_partitioned_queue',
    :'partition_interval',
    :'retention_interval'
);

-- queue shows up in list queues
SELECT queue_name FROM pgmq.list_queues()
 WHERE queue_name = 'test_partitioned_queue';

-- Sending 3 messages to the queue
SELECT send AS msg_id1 from pgmq.send('test_partitioned_queue', '0') \gset
SELECT send AS msg_id2 from pgmq.send('test_partitioned_queue', '0') \gset
SELECT send AS msg_id3 from pgmq.send('test_partitioned_queue', '0') \gset

SELECT COUNT(*) = 3 FROM pgmq.q_test_partitioned_queue;

-- Deleting message 3
SELECT * FROM pgmq.delete('test_partitioned_queue', :msg_id3);
SELECT COUNT(*) = 2 FROM pgmq.q_test_partitioned_queue;

-- Deleting batch
SELECT ARRAY(
    SELECT archive FROM pgmq.archive(
        'test_partitioned_queue',
        ARRAY[:msg_id1, :msg_id2, :msg_id3, -3]
    )
) = ARRAY[:msg_id1, :msg_id2]::bigint[];

-- test_transaction_create
BEGIN;
SELECT pgmq.create('transaction_test_queue');
ROLLBACK;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'q_transaction_test_queue';

-- test_detach_archive
SELECT pgmq.create('detach_archive_queue');
DROP EXTENSION pgmq CASCADE;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'a_detach_archive_queue';

-- With detach, archive remains
CREATE EXTENSION pgmq;
SELECT pgmq.create('detach_archive_queue');
SELECT pgmq.detach_archive('detach_archive_queue');
DROP EXTENSION pgmq CASCADE;
SELECT tablename FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'a_detach_archive_queue';

--Truncated Index When queue name is max.
CREATE EXTENSION pgmq;
SELECT pgmq.create('long_queue_name_123456789012345678901234567890');
SELECT pgmq.convert_archive_partitioned('long_queue_name_123456789012345678901234567890');

--Check for archive is already partitioned
SELECT pgmq.convert_archive_partitioned('long_queue_name_123456789012345678901234567890');

--Error out due to Index duplicate index at old table.
SELECT pgmq.create('long_queue_name_1234567890123456789012345678901');
SELECT pgmq.convert_archive_partitioned('long_queue_name_1234567890123456789012345678901');

--Success
SELECT pgmq.create('long_queue_name_');
SELECT pgmq.convert_archive_partitioned('long_queue_name_');

\set SHOW_CONTEXT never

--Failed SQL injection attack
SELECT pgmq.create('abc');
SELECT
   pgmq.delete(
     'abc where false;
     create table public.attack_vector(id int);
     delete from pgmq.q_abc',
     1
  );

--Special characters in queue name
SELECT pgmq.create('queue-hyphened');
SELECT pgmq.send('queue-hyphened', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('queue-hyphened', 1, 1);
SELECT pgmq.archive('queue-hyphened', 1);

SELECT pgmq.create('QueueCased');
SELECT pgmq.send('QueueCased', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('QueueCased', 1, 1);
SELECT pgmq.archive('QueueCased', 1);

SELECT pgmq.create_partitioned('queue-hyphened-part');
SELECT pgmq.send('queue-hyphened-part', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('queue-hyphened-part', 1, 1);
SELECT pgmq.archive('queue-hyphened-part', 1);

SELECT pgmq.create_partitioned('QueueCasedPart');
SELECT pgmq.send('QueueCasedPart', '{"hello":"world"}');
SELECT msg_id, read_ct, message FROM pgmq.read('QueueCasedPart', 1, 1);
SELECT pgmq.archive('QueueCasedPart', 1);

-- fails with invalid queue name
SELECT pgmq.create('dollar$-signed');
SELECT pgmq.create_partitioned('dollar$-signed-part');

-- input validation success
SELECT pgmq.format_table_name('cat', 'q');
SELECT pgmq.format_table_name('cat-dog', 'a');
SELECT pgmq.format_table_name('cat_dog', 'q');

-- input validation failure
SELECT pgmq.format_table_name('dollar$fail', 'q');
SELECT pgmq.format_table_name('double--hyphen-fail', 'a');
SELECT pgmq.format_table_name('semicolon;fail', 'a');
SELECT pgmq.format_table_name($$single'quote-fail$$, 'a');

--Cleanup tests
DROP EXTENSION pgmq CASCADE;
DROP EXTENSION pg_partman CASCADE;
