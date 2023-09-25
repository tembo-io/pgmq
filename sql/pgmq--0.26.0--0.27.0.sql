-- Dropping removed functions
DROP FUNCTION pgmq.send(text, jsonb, integer);
DROP FUNCTION pgmq.send_batch(text, jsonb[], integer);
