-- Test FIFO message queue functionality
CREATE EXTENSION IF NOT EXISTS pgmq;

SELECT pgmq.create('fifo_queue');

SELECT * FROM pgmq.send('fifo_queue', '"hello"'::jsonb, '{"x-pgmq-fifo": "key-1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{"x-pgmq-fifo": "key-2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{"x-pgmq-fifo": "key-2"}'::jsonb);
SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{}'::jsonb);

SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_queue', 10, 10);

SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{"x-pgmq-fifo": "key-1"}'::jsonb);
SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{}'::jsonb);
SELECT * FROM pgmq.send('fifo_queue', '"fifo"'::jsonb, '{"x-pgmq-fifo": "key-3"}'::jsonb);

-- Second read should only get the message with key-3, as key-1 (and no key) still
-- have messages with active visibility timeouts
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_queue', 10, 10);

-- Expire VT for all messages
UPDATE pgmq.q_fifo_queue SET vt = CURRENT_TIMESTAMP;

-- After the VT expires we should be able to read all messages
SELECT msg_id, message, headers FROM pgmq.read_fifo('fifo_queue', 10, 10);

SELECT pgmq.drop_queue('fifo_queue');
