import asyncio
import random

async def producer(queue, num_tasks):
    for i in range(num_tasks):
        item = f"Task {i}"
        await queue.put(item) # Blocks if queue is full
        print(f"Produced {item}")
    # # Signal consumers to stop by sending None or a sentinel value
    # for _ in range(num_tasks): # Send a sentinel for each consumer if necessary
    #     await queue.put(None)

async def consumer(queue, name):
    while True:
        item = await queue.get() # Blocks if queue is empty
        print("item: ", item)
        if item is None:
            queue.task_done()
        print(f"{name} processing {item}")
        await asyncio.sleep(random.random()) # Simulate work
        queue.task_done() # Mark the task as done

    # queue.task_done() # Mark the sentinel as done

async def main():
    queue = asyncio.Queue(maxsize=5) # Bounded queue
    num_tasks = 10
    consumers = [asyncio.create_task(consumer(queue, f"Consumer {i+1}")) for i in range(2)]
    
    # Start producer and wait for all tasks to be processed
    await producer(queue, num_tasks)
    await queue.join() # Wait until all items have been processed

    # Cancel consumer tasks (if using sentinel None logic)
    for c in consumers:
        c.cancel()
    print("Finished!")
    await asyncio.gather(*consumers, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
