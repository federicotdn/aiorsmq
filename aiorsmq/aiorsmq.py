from typing import (
    Text,
    List,
    Dict,
    Optional,
    Callable,
    Awaitable,
    Any,
    TYPE_CHECKING,
)
from functools import wraps

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


class Queue:
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
        total_recv: Optional[int] = None,
        total_sent: Optional[int] = None,
        created: Optional[int] = None,
        modified: Optional[int] = None,
    ) -> None:
        self.vt = vt
        self.delay = delay
        self.max_size = max_size
        self.total_recv = total_recv
        self.total_sent = total_sent
        self.created = created
        self.modified = modified


class AIORSMQ:
    def __init__(self, *, client: "Redis", ns: Text = compat.DEFAULT_NAMESPACE) -> None:
        self._client = client
        self._ns = ns
        self._initialized = False

        self._script_pop_message = None
        self._script_receive_message = None
        self._script_change_message_visibility = None

    def connected(
        method: Callable[..., Awaitable[Any]]
    ) -> Callable[..., Awaitable[Any]]:
        @wraps(method)
        async def inner(self, *args: List, **kwargs: Dict) -> Any:
            await self._initialize()
            return await method(self, *args, **kwargs)

        return inner

    async def _initialize(self) -> None:
        if self._initialized:
            return

        self._script_pop_message = self._client.register_script(scripts.POP_MESSAGE)
        self._script_receive_message = self._client.register_script(
            scripts.RECEIVE_MESSAGE
        )
        self._script_change_message_visibility = self._client.register_script(
            scripts.CHANGE_MESSAGE_VISIBILITY
        )

        self._initialized = True

    async def _get_queue_and_uid(self, queue_name: Text) -> Dict[Text, int]:
        key = compat.queue_name(self._ns, queue_name)
        pipeline = self._client.pipeline()

        pipeline.hmget(key, [compat.VT, compat.DELAY, compat.MAX_SIZE])
        pipeline.time()

        result = await pipeline.execute()

        if any([v is None for v in result[0]]):
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

        ms: Text = compat.format_zero_pad(result[1][1], 6)
        uid: Text = compat.base36_encode(int(str(result[1][0]) + ms)) + compat.make_id()
        ts: int = int(str(result[1][0]) + ms[0:3])

        return {
            compat.VT: int(result[0][0]),
            compat.DELAY: int(result[0][1]),
            compat.MAX_SIZE: int(result[0][2]),
            "ts": ts,
            "uid": uid,
        }

    @connected
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

    @connected
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

    async def get_queue_attributes(self, queue_name: Text) -> Dict[Text, int]:
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
        pass

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
