# https://github.com/postgres/postgres/tree/master/src/test/isolation

setup {
  CREATE EXTENSION IF NOT EXISTS pg_partman;
  CREATE EXTENSION IF NOT EXISTS pgmq;
}

# test_transaction_send
session conn1
step c1create { SELECT pgmq.create('transaction_test_queue'); }
step c1begin { BEGIN; }
step c1send  { SELECT * FROM pgmq.send('transaction_test_queue', '1'); }
step c1commit { COMMIT; }
step c1delete { SELECT * FROM pgmq.delete('transaction_test_queue', 1); }
step c1read { SELECT msg_id FROM pgmq.read('transaction_test_queue', 0, 1) }
step c1rollback { ROLLBACK; }

session conn2
step c2begin { BEGIN; }
step c2read { SELECT msg_id FROM pgmq.read('transaction_test_queue', 0, 1) }

# test_transaction_send
permutation c1create c1begin c1send c2read c1commit c2read c1delete

# test_transaction_read
permutation c1send c1begin c2begin c1read c2read c1rollback c2read
