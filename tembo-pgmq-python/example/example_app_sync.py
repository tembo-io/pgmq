from tembo_pgmq_python.queue import PGMQueue
from tembo_pgmq_python.decorators import transaction

queue = PGMQueue(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
    verbose=True,
    log_filename="pgmq_sync.log",
)

test_queue = "transaction_queue_sync"

# Clean up if the queue already exists
queues = queue.list_queues()
if test_queue in queues:
    queue.drop_queue(test_queue)
queue.create_queue(test_queue)

# Example messages
messages = [
    {"id": 1, "content": "First message"},
    {"id": 2, "content": "Second message"},
    {"id": 3, "content": "Third message"},
]


# Create table function for non-PGMQ DB operation
def create_mytable(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mytable (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL
                )
            """)
            print("Table 'mytable' created or already exists.")
    except Exception as e:
        print(f"Failed to create table 'mytable': {e}")
        raise


# Transaction with only PGMQ operations
@transaction
def pgmq_operations(queue, conn=None):
    # Send multiple messages
    msg_ids = queue.send_batch(
        test_queue,
        messages=messages,
        conn=conn,
    )
    print(f"PGMQ: Messages sent with IDs: {msg_ids}")

    # Read messages within the transaction
    internal_messages = queue.read_batch(
        test_queue,
        batch_size=10,
        conn=conn,
    )
    print(f"PGMQ: Messages read within transaction: {internal_messages}")


# Transaction with non-PGMQ DB operation and PGMQ operation - Success case
@transaction
def non_pgmq_db_operations_success(queue, conn=None):
    create_mytable(conn)

    # Non-PGMQ database operation (simulating a custom DB operation)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO mytable (name) VALUES ('Alice')")
        print("Non-PGMQ DB: Inserted into 'mytable'.")

    # Send multiple PGMQ messages
    msg_ids = queue.send_batch(
        test_queue,
        messages=messages,
        conn=conn,
    )
    print(f"PGMQ: Messages sent with IDs: {msg_ids}")


# Transaction with non-PGMQ DB operation and PGMQ operation - Failure case
@transaction
def non_pgmq_db_operations_failure(queue, conn=None):
    create_mytable(conn)

    # Non-PGMQ database operation (simulating a custom DB operation)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO mytable (name) VALUES ('Bob')")
        print("Non-PGMQ DB: Inserted into 'mytable'.")

    # Simulating a failure after a PGMQ operation
    raise Exception(
        "Simulated failure after inserting into mytable and sending messages"
    )


# Transaction with PGMQ operations and non-database operation (simple print statement)
@transaction
def non_db_operations(queue, conn=None):
    # Send multiple messages
    msg_ids = queue.send_batch(
        test_queue,
        messages=messages,
        conn=conn,
    )
    print(f"PGMQ: Messages sent with IDs: {msg_ids}")

    # Non-database operation: Print statement
    print("Non-DB: Simulating a non-database operation (printing).")


# Transaction failure: only delete if queue size is larger than threshold
@transaction
def conditional_failure(queue, conn=None):
    # Send multiple messages within the transaction
    msg_ids = queue.send_batch(
        test_queue,
        messages=messages,
        conn=conn,
    )
    print(f"Messages sent with IDs: {msg_ids}")

    # Read messages currently in the queue within the transaction
    messages_in_queue = queue.read_batch(
        test_queue,
        batch_size=10,
        conn=conn,
    )
    print(
        f"Messages currently in queue before conditional failure: {messages_in_queue}"
    )

    # Simulate a condition: only delete if the queue has more than 3 messages
    if len(messages_in_queue) > 3:
        queue.delete(
            test_queue,
            msg_id=messages_in_queue[0].msg_id,
            conn=conn,
        )
        print(f"Message ID {messages_in_queue[0].msg_id} deleted within transaction.")
    else:
        # Simulate a failure if the queue size is not greater than 3
        print(
            "Transaction failed: Not enough messages in queue to proceed with deletion."
        )
        raise Exception("Queue size too small to proceed.")

    print("Transaction completed successfully.")


# Transaction success for conditional scenario
@transaction
def conditional_success(queue, conn=None):
    # Send additional messages to ensure the queue has more than 3 messages
    additional_messages = [
        {"id": 4, "content": "Fourth message"},
        {"id": 5, "content": "Fifth message"},
    ]
    msg_ids = queue.send_batch(
        test_queue,
        messages=additional_messages,
        conn=conn,
    )
    print(f"Messages sent with IDs: {msg_ids}")

    # Read messages currently in the queue within the transaction
    messages_in_queue = queue.read_batch(
        test_queue,
        batch_size=10,
        conn=conn,
    )
    print(
        f"Messages currently in queue before successful conditional deletion: {messages_in_queue}"
    )

    # Proceed with deletion if more than 3 messages are in the queue
    if len(messages_in_queue) > 3:
        queue.delete(
            test_queue,
            msg_id=messages_in_queue[0].msg_id,
            conn=conn,
        )
        print(f"Message ID {messages_in_queue[0].msg_id} deleted within transaction.")

    print("Conditional success transaction completed.")


# Read messages after transaction to see if changes were committed
def read_queue_after_transaction():
    external_messages = queue.read_batch(test_queue, batch_size=10)
    if external_messages:
        print("Messages read after transaction:")
        for msg in external_messages:
            print(f"ID: {msg.msg_id}, Content: {msg.message}")
    else:
        print("No messages found after transaction rollback.")


# Execute transactions and handle exceptions
print("=== Executing PGMQ Operations ===")
try:
    pgmq_operations(queue)
except Exception as e:
    print(f"PGMQ Transaction failed: {e}")

print("\n=== Executing Non-PGMQ DB and PGMQ Operations (Success Case) ===")
try:
    non_pgmq_db_operations_success(queue)
except Exception as e:
    print(f"Non-PGMQ DB Transaction failed: {e}")

print("\n=== Executing Non-PGMQ DB and PGMQ Operations (Failure Case) ===")
try:
    non_pgmq_db_operations_failure(queue)
except Exception as e:
    print(f"Non-PGMQ DB Transaction failed: {e}")

print("\n=== Executing Non-DB and PGMQ Operations ===")
try:
    non_db_operations(queue)
except Exception as e:
    print(f"Non-DB Transaction failed: {e}")

print("\n=== Reading Queue After Transactions ===")
read_queue_after_transaction()

# Purge the queue for failure case
queue.purge(test_queue)

print("\n=== Executing Conditional Failure Scenario ===")
try:
    conditional_failure(queue)
except Exception as e:
    print(f"Conditional Failure Transaction failed: {e}")
read_queue_after_transaction()

print("\n=== Executing Conditional Success Scenario ===")
try:
    conditional_success(queue)


except Exception as e:
    print(f"Conditional Success Transaction failed: {e}")
    read_queue_after_transaction()

# Read the queue after the conditional failure and success
print("\n=== Reading Queue After Conditional Scenarios ===")
read_queue_after_transaction()

# Clean up
queue.drop_queue(test_queue)
