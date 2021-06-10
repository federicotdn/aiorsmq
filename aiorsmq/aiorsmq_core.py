from typing import (
    Text,
    List,
    Dict,
    Optional,
    NamedTuple,
    TYPE_CHECKING,
)

from aiorsmq import scripts, exceptions, compat

if TYPE_CHECKING:
    from aioredis.client import Redis


class Message:
    __slots__ = ["message", "id", "sent", "fr", "rc"]

    def __init__(self, *, message: Text, id: Text, fr: int, rc: int) -> None:
        self.message = message
        self.id = id
        self.fr = fr
        self.rc = rc


class QueueAttributes:
    __slots__ = [
        "vt",
        "delay",
        "max_size",
        "total_recv",
        "total_sent",
        "created",
        "modified",
    ]

    def __init__(
        self,
        *,
        vt: int,
        delay: int,
        max_size: int,
        total_recv: int,
        total_sent: int,
        created: int,
        modified: int,
    ) -> None:
        self.vt = vt
        self.delay = delay
        self.max_size = max_size
        self.total_recv = total_recv
        self.total_sent = total_sent
        self.created = created
        self.modified = modified


class AIORSMQCore:
    class QueueContext(NamedTuple):
        vt: int
        delay: int
        max_size: int
        ts: int
        uid: Text

    def __init__(
        self,
        *,
        client: "Redis",
        ns: Text = compat.DEFAULT_NAMESPACE,
        real_time: bool = False,
    ) -> None:
        self._client = client
        self._ns = ns
        self._real_time = real_time

        self._script_pop_message = self._client.register_script(scripts.POP_MESSAGE)
        self._script_receive_message = self._client.register_script(
            scripts.RECEIVE_MESSAGE
        )
        self._script_change_message_visibility = self._client.register_script(
            scripts.CHANGE_MESSAGE_VISIBILITY
        )

    async def _get_queue_context(self, queue_name: Text) -> "AIORSMQCore.QueueContext":
        key = compat.queue_name(self._ns, queue_name)
        pipeline = self._client.pipeline()

        pipeline.hmget(key, [compat.VT, compat.DELAY, compat.MAX_SIZE])
        pipeline.time()

        result = await pipeline.execute()

        if any([v is None for v in result[0]]):
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

        unix_time: int = result[1][0]
        microseconds: int = result[1][1]

        ms: Text = compat.format_zero_pad(microseconds, 6)
        uid: Text = compat.base36_encode(int(str(unix_time) + ms)) + compat.make_id()
        ts: int = (unix_time * 1000) + (microseconds // 1000)

        return AIORSMQCore.QueueContext(
            vt=int(result[0][0]),
            delay=int(result[0][1]),
            max_size=int(result[0][2]),
            ts=ts,
            uid=uid,
        )

    async def create_queue(
        self,
        queue_name: Text,
        vt: int = compat.DEFAULT_VT,
        delay: int = compat.DEFAULT_DELAY,
        max_size: int = compat.DEFAULT_MAX_SIZE,
    ) -> None:
        key = compat.queue_name(self._ns, queue_name)
        pipeline = self._client.pipeline()
        now = await self._client.time()

        pipeline.hsetnx(key, compat.VT, vt)
        pipeline.hsetnx(key, compat.DELAY, delay)
        pipeline.hsetnx(key, compat.MAX_SIZE, max_size)
        pipeline.hsetnx(key, compat.CREATED, now[0])
        pipeline.hsetnx(key, compat.MODIFIED, now[0])

        result = await pipeline.execute()

        if result[0] == 0:
            raise exceptions.QueueExistsException(
                f"Queue '{queue_name}' already exists."
            )

        await self._client.sadd(compat.queues_set(self._ns), queue_name)

    async def list_queues(self) -> List[Text]:
        queues = await self._client.smembers(compat.queues_set(self._ns))
        return list(queues)

    async def delete_queue(self, queue_name: Text) -> None:
        keys = [
            compat.queue_name(self._ns, queue_name),
            compat.queue_name(self._ns, queue_name, with_q=False),
        ]

        pipeline = self._client.pipeline()
        pipeline.delete(*keys)
        pipeline.srem(compat.queues_set(self._ns), queue_name)

        result = await pipeline.execute()

        if result[0] == 0:
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

    async def get_queue_attributes(self, queue_name: Text) -> QueueAttributes:
        pass

    async def set_queue_attributes(
        self,
        queue_name: Text,
        vt: Optional[int] = None,
        delay: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> Dict[Text, int]:
        pass

    async def send_message(
        self, queue_name: Text, message: Text, delay: int = 0
    ) -> Text:
        context = await self._get_queue_context(queue_name)

        # TODO: Check params

        key_base = compat.queue_name(self._ns, queue_name, False)
        key_queue = compat.queue_name(self._ns, queue_name)

        pipeline = self._client.pipeline()

        pipeline.zadd(key_base, {context.uid: context.ts + delay * 1000})
        pipeline.hset(key_queue, context.uid, message)
        pipeline.hincrby(key_queue, compat.TOTAL_SENT, 1)

        if self._real_time:
            pipeline.zcard(key_base)

        result = await pipeline.execute()

        if self._real_time:
            await self._client.publish(compat.queue_rt(self._ns, queue_name), result[3])

        return context.uid

    async def receive_message(
        self, queue_name: Text, vt: Optional[int] = None
    ) -> Optional[Message]:
        pass

    async def delete_message(self, queue_name: Text, id: Text) -> None:
        pass

    async def pop_message(self, queue_name: Text, id: Text) -> None:
        pass

    async def change_message_visibility(
        self, queue_name: Text, id: Text, vt: int
    ) -> None:
        pass
