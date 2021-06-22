import asyncio  # For the .run() function
import aioredis  # For creating the connection to Redis
from aiorsmq import AIORSMQ


async def send_message():
    client = aioredis.from_url(url="redis://localhost:6379", decode_responses=True)
    rsmq = AIORSMQ(client=client)

    await rsmq.create_queue("my-queue")
    await rsmq.send_message("my-queue", "Hello, world!")


asyncio.run(send_message())
