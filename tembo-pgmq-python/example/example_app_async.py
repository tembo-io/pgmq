import asyncio
from tembo_pgmq_python.async_queue import PGMQueue
from tembo_pgmq_python.decorators import async_transaction as transaction


async def main():
    # Initialize the queue
    queue = PGMQueue(
        host="0.0.0.0",
        port="5432",
        username="postgres",
        password="postgres",
        database="postgres",
        verbose=True,
        log_filename="pgmq_async.log",
    )
    await queue.init()

    test_queue = "transactional_queue_async"

    # Clean up if the queue already exists
    queues = await queue.list_queues()
    if test_queue in queues:
        await queue.drop_queue(test_queue)
    await queue.create_queue(test_queue)

    # Example messages
    message1 = {"id": 1, "content": "First message"}
    message2 = {"id": 2, "content": "Second message"}

    # Transactional operation: send messages within a transaction
    @transaction
    async def transactional_operation(queue: PGMQueue, conn=None):
        # Perform multiple queue operations within a transaction
        await queue.send(test_queue, message1, conn=conn)
        await queue.send(test_queue, message2, conn=conn)
        # If an exception occurs here, all previous operations will be rolled back

    # Execute the transactional function (Success Case)
    try:
        await transactional_operation(queue)
        print("Transaction committed successfully.")
    except Exception as e:
        print(f"Transaction failed: {e}")

    # Read messages outside of the transaction
    read_message1 = await queue.read(test_queue)
    read_message2 = await queue.read(test_queue)
    print("Messages read after transaction commit:")
    if read_message1:
        print(f"Message 1: {read_message1.message}")
    if read_message2:
        print(f"Message 2: {read_message2.message}")

    # Purge the queue for the failure case
    await queue.purge(test_queue)

    # Transactional operation: simulate failure
    @transaction
    async def transactional_operation_failure(queue: PGMQueue, conn=None):
        await queue.send(test_queue, message1, conn=conn)
        await queue.send(test_queue, message2, conn=conn)
        # Simulate an error to trigger rollback
        raise Exception("Simulated failure")

    # Execute the transactional function (Failure Case)
    try:
        await transactional_operation_failure(queue)
    except Exception as e:
        print(f"Transaction failed: {e}")

    # Attempt to read messages after failed transaction
    read_message = await queue.read(test_queue)
    if read_message:
        print("Message read after failed transaction (should not exist):")
        print(read_message.message)
    else:
        print("No messages found after transaction rollback.")

    # Clean up
    await queue.drop_queue(test_queue)
    await queue.pool.close()


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
