ALTER TABLE pgmq.meta ADD COLUMN is_unlogged BOOLEAN;
UPDATE pgmq.meta SET is_unlogged = false;
ALTER TABLE pgmq.meta ALTER COLUMN is_unlogged SET NOT NULL;
