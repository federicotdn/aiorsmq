import asyncio
import aioredis
from aiorsmq import AIORSMQ


async def receive_message():
    client = aioredis.from_url(url="redis://localhost:6379", decode_responses=True)
    rsmq = AIORSMQ(client=client)

    message = await rsmq.receive_message("my-queue")
    print("The message is:", message.contents)

    await rsmq.delete_message("my-queue", message.id)


asyncio.run(receive_message())
