


drop extension pg_smq;
create extension pg_smq;
select psmq_create('test');
select psmq_enqueue('test', '{"x": 1}');
select psmq_read('test');


WITH cte AS
    (
        SELECT *
        FROM test
        WHERE vt <= now() at time zone 'utc'
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
UPDATE test
SET vt = (now() at time zone 'utc' + interval '10 seconds')
WHERE msg_id = (select msg_id from cte)
RETURNING *;



WITH cte AS
    (
        SELECT msg_id, vt, message
        FROM testing
        WHERE vt <= now() at time zone 'utc'
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
DELETE from testing
WHERE msg_id = (select msg_id from cte)
RETURNING *;