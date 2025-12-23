import asyncio

async def test_task(item):
    items=[]
    await asyncio.sleep(1) # Work
                
    # Add new work to LOCAL list, not a new task
    # if item < 20:
    # enqueue
    for i in range(10):
        pass
        # items.append(f"{item}")
    return items