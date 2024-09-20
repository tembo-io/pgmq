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
    queue.drop_queue(test_queue)  # Pass queue name as positional argument
queue.create_queue(test_queue)  # Pass queue name as positional argument

# Example messages
messages = [
    {"id": 1, "content": "First message"},
    {"id": 2, "content": "Second message"},
    {"id": 3, "content": "Third message"},
]


# Transactional operation: send multiple messages and perform additional operations within a transaction
@transaction
def transactional_operations(queue, conn=None):
    # Send multiple messages
    msg_ids = queue.send_batch(
        test_queue,  # Positional argument
        messages=messages,
        conn=conn,
    )
    print(f"Messages sent with IDs: {msg_ids}")

    # Read messages within the transaction
    internal_messages = queue.read_batch(
        test_queue,  # Positional argument
        batch_size=10,
        conn=conn,
    )
    print(f"Messages read within transaction: {internal_messages}")

    # Perform additional operations
    if msg_ids:
        queue.delete(
            test_queue,  # Positional argument
            msg_id=msg_ids[0],
            conn=conn,
        )
        print(f"Deleted message ID: {msg_ids[0]} within transaction")


# Execute the transactional operations (Success Case)
print("=== Executing Transactional Operations (Success Case) ===")
try:
    transactional_operations(queue)
except Exception as e:
    print(f"Transaction failed: {e}")

# Read messages after transaction commit
external_messages = queue.read_batch(test_queue, batch_size=10)
print("Messages read after transaction commit:")
for msg in external_messages:
    print(f"ID: {msg.msg_id}, Content: {msg.message}")

# Clean up for failure case
queue.purge(test_queue)


# Transactional operation: simulate failure
@transaction
def transactional_operations_failure(queue, conn=None):
    # Send multiple messages
    msg_ids = queue.send_batch(
        test_queue,  # Positional argument
        messages=messages,
        conn=conn,
    )
    print(f"Messages sent with IDs: {msg_ids}")

    # Simulate an error to trigger a rollback
    raise Exception("Simulated failure in transactional operations")


# Execute the transactional operations (Failure Case)
print("\n=== Executing Transactional Operations (Failure Case) ===")
try:
    transactional_operations_failure(queue)
except Exception as e:
    print(f"Transaction failed: {e}")

# Read messages after transaction rollback
external_messages = queue.read_batch(test_queue, batch_size=10)
if external_messages:
    print("Messages read after transaction rollback:")
    for msg in external_messages:
        print(f"ID: {msg.msg_id}, Content: {msg.message}")
else:
    print("No messages found after transaction rollback.")

# Clean up
queue.drop_queue(test_queue)
