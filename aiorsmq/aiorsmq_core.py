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


class Message(NamedTuple):
    message: Text
    id: Text
    fr: int
    rc: int
    sent: float


class QueueAttributes(NamedTuple):
    vt: int
    delay: int
    max_size: int
    total_recv: int
    total_sent: int
    created: int
    modified: int


class AIORSMQCore:
    class QueueContext(NamedTuple):
        vt: int
        delay: int
        max_size: int
        ts: int
        uid: Optional[Text]

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

    async def _get_queue_context(
        self, queue_name: Text, add_uid: bool = False
    ) -> "AIORSMQCore.QueueContext":
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

        uid: Optional[Text] = None
        ts: int = (unix_time * 1000) + (microseconds // 1000)

        if add_uid:
            ms: Text = compat.format_zero_pad(microseconds, 6)
            uid = compat.base36_encode(int(str(unix_time) + ms)) + compat.make_id()

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
        # TODO: Validate params
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
        self, queue_name: Text, message: Text, delay: Optional[int] = None
    ) -> Text:
        # TODO: Validate params
        context = await self._get_queue_context(queue_name, add_uid=True)
        delay = context.delay if delay is None else delay

        key_base = compat.queue_name(self._ns, queue_name, with_q=False)
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

    async def pop_message(self, queue_name: Text) -> None:
        context = await self._get_queue_context(queue_name)
        key_base = compat.queue_name(self._ns, queue_name, with_q=False)

        result = await self._script_pop_message(keys=[key_base, context.ts])
        if not result:
            return None

        return Message(
            message=result[1],
            id=result[0],
            fr=int(result[3]),
            rc=result[2],
            sent=compat.base36_decode(result[0][:10]) / 1000,
        )

    async def change_message_visibility(
        self, queue_name: Text, id: Text, vt: int
    ) -> None:
        # TODO: Validate params
        context = await self._get_queue_context(queue_name)
        key_base = compat.queue_name(self._ns, queue_name, with_q=False)

        result = await self._script_change_message_visibility(
            keys=[key_base, id, context.ts + vt * 1000]
        )

        if result == 0:
            raise exceptions.MessageNotFoundException(
                f"Message with ID '{id}' does not exist."
            )

    async def quit(self) -> None:
        await self._client.close()
