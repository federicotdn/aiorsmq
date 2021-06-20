from typing import (
    List,
    Optional,
    Union,
    NamedTuple,
)
import re

import aioredis  # type: ignore

from aiorsmq import scripts, exceptions, compat, utils


class Message:
    """Represents a message received from a message queue."""

    __slots__ = ["message", "id", "fr", "rc", "sent"]

    def __init__(
        self, *, message: Union[str, bytes], id: str, fr: int, rc: int, sent: float
    ) -> None:
        """Initialize a `Message` object.

        **Note:** This description is provided only for documentation purposes - users
        of `aiorsmq` have no need for creating `Message` objects manually.

        Args:
            message: Contents of the message.
            id: Message's unique ID, consisting of 32 characters. Example:
                `fzl7ufz2q5iX40Bm9uc0DjQCsqpbQfL3`.
            fr: UNIX timestamp indicating when the message was first receieved (in
                milliseconds).
            rc: Number of times this message has been received by
                consumers. Will always be at least 1.
            sent: UNIX timestamp indicating when the message was sent (in milliseconds).
        """
        self.message = message
        self.id = id
        self.fr = fr
        self.rc = rc
        self.sent = sent


class QueueAttributes:
    """Represents the attributes of a message queue."""

    __slots__ = [
        "vt",
        "delay",
        "max_size",
        "total_recv",
        "total_sent",
        "created",
        "modified",
        "messages",
        "hidden_messages",
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
        messages: int,
        hidden_messages: int,
    ) -> None:
        """Initialize a `QueueAttributes` object.

        **Note:** This description is provided only for documentation purposes - users
        of `aiorsmq` have no need for creating `QueueAttributes` objects manually.

        Args:
            vt:
            delay:
            max_size:
            total_recv:
            total_sent:
            created:
            modified:
            messaged:
            hidden_messages:
        """
        self.vt = vt
        self.delay = delay
        self.max_size = max_size
        self.total_recv = total_recv
        self.total_sent = total_sent
        self.created = created
        self.modified = modified
        self.messages = messages
        self.hidden_messages = hidden_messages


class _QueueContext(NamedTuple):
    vt: int
    delay: int
    max_size: int
    ts: int
    uid: Optional[str]


class AIORSMQ:
    """Asynchronous Python implementation of the JavaScript `rsmq` (Redis Simple
    Message Queue) library.

    The `AIORSMQ` class uses a connection to a Redis server to create and
    delete message queues, as well as send and receive messages. It is the main entry
    point of the `aiorsmq` package.

    """

    def __init__(
        self,
        *,
        client: aioredis.Redis,
        client_encoding: str = "utf-8",
        namespace: str = compat.DEFAULT_NAMESPACE,
        real_time: bool = False,
    ) -> None:
        """Initialize an `AIORSMQ` object.

        Args:
            client: Redis client to use internally (create it using the `aioredis`
                package).
            client_encoding: When the Redis client has been configured with
                `decode_responses=True`, please ensure that the value of this parameter
                matches the encoding used for the Redis client. When the Redis client
                has not been configured with `decode_responses=True`, you may safely
                ignore this parameter. The default encoding used by the Redis client is
                also `utf-8`, so in most cases setting this parameter manually will not
                be necessary.
            namespace: Namespace to prefix keys with.
            real_time: Enable real time mode. When enabled, a notification will be
                sent using Redis `PUBLISH` each time a message is added to a message
                queue.
        """
        self._client = client
        self._client_encoding = client_encoding
        self._ns = namespace
        self._real_time = real_time

        self._script_pop_message = self._client.register_script(scripts.POP_MESSAGE)
        self._script_receive_message = self._client.register_script(
            scripts.RECEIVE_MESSAGE
        )
        self._script_change_message_visibility = self._client.register_script(
            scripts.CHANGE_MESSAGE_VISIBILITY
        )

    @staticmethod
    def _validate(
        queue_name: Optional[str] = None,
        id: Optional[str] = None,
        vt: Optional[int] = None,
        delay: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> None:
        if queue_name is not None and not re.match(compat.QUEUE_NAME_RE, queue_name):
            raise exceptions.InvalidValueException("Incorrect format for queue name.")

        if id is not None and not re.match(compat.ID_RE, id):
            raise exceptions.InvalidValueException("Incorrect format for message ID.")

        if vt is not None and not (compat.MIN_VT <= vt <= compat.MAX_VT):
            raise exceptions.InvalidValueException("Incorrect value for vt parameter.")

        if delay is not None and not (compat.MIN_DELAY <= delay <= compat.MAX_DELAY):
            raise exceptions.InvalidValueException(
                "Incorrect value for delay parameter."
            )

        if max_size is not None and not (
            max_size == compat.MAX_SIZE_UNLIMITED
            or (compat.MIN_MAX_SIZE <= max_size <= compat.MAX_MAX_SIZE)
        ):
            raise exceptions.InvalidValueException(
                "Incorrect value for max_size parameter."
            )

    async def _get_queue_context(
        self, queue_name: str, add_uid: bool = False
    ) -> _QueueContext:
        key_hash = compat.queue_hash(self._ns, queue_name)
        pipeline = self._client.pipeline()

        pipeline.hmget(key_hash, [compat.VT, compat.DELAY, compat.MAX_SIZE])
        pipeline.time()

        result = await pipeline.execute()

        if any([v is None for v in result[0]]):
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

        unix_time: int = result[1][0]
        microseconds: int = result[1][1]

        uid: Optional[str] = None
        ts: int = (unix_time * 1000) + (microseconds // 1000)

        if add_uid:
            uid = compat.message_uid(unix_time, microseconds)

        return _QueueContext(
            vt=int(result[0][0]),
            delay=int(result[0][1]),
            max_size=int(result[0][2]),
            ts=ts,
            uid=uid,
        )

    async def create_queue(
        self,
        queue_name: str,
        vt: int = compat.DEFAULT_VT,
        delay: int = compat.DEFAULT_DELAY,
        max_size: int = compat.DEFAULT_MAX_SIZE,
    ) -> None:
        """Create a new message queue.

        Args:
            queue_name: Name of the new message queue.
            vt: Default visibility delay (in seconds) to use when receiving messages
                from the queue.
            delay: Default delay (in seconds) to apply when sending messages to the
                queue.
            max_size: Maximum message size for the queue (in bytes).

        Raises:
            exceptions.QueueExistsException: When a queue with the given name already
                exists.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.
        """
        self._validate(queue_name=queue_name, vt=vt, delay=delay, max_size=max_size)

        key_hash = compat.queue_hash(self._ns, queue_name)
        pipeline = self._client.pipeline()
        now = await self._client.time()

        pipeline.hsetnx(key_hash, compat.VT, vt)
        pipeline.hsetnx(key_hash, compat.DELAY, delay)
        pipeline.hsetnx(key_hash, compat.MAX_SIZE, max_size)
        pipeline.hsetnx(key_hash, compat.CREATED, now[0])
        pipeline.hsetnx(key_hash, compat.MODIFIED, now[0])

        result = await pipeline.execute()

        if result[0] == 0:
            raise exceptions.QueueExistsException(
                f"Queue '{queue_name}' already exists."
            )

        await self._client.sadd(compat.queues_set(self._ns), queue_name)

    async def list_queues(self) -> List[str]:
        """Retrieve a list of all existing queues.

        **Note:** The namespace is **not** included in the queue names.

        Returns:
            List of queue names.
        """
        queues = await self._client.smembers(compat.queues_set(self._ns))
        return list(queues)

    async def delete_queue(self, queue_name: str) -> None:
        """Delete a message queue.

        Args:
            queue_name: Name of the message queue to delete.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.
        """
        self._validate(queue_name=queue_name)

        keys = [
            compat.queue_sorted_set(self._ns, queue_name),
            compat.queue_hash(self._ns, queue_name),
        ]

        pipeline = self._client.pipeline()
        pipeline.delete(*keys)
        pipeline.srem(compat.queues_set(self._ns), queue_name)

        result = await pipeline.execute()

        if result[0] == 0:
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

    async def get_queue_attributes(self, queue_name: str) -> QueueAttributes:
        """Retrieve a message queue's attributes.

        Args:
            queue_name: Name of the message queue.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.

        Returns:
            Object containing the queue's attributes.
        """
        self._validate(queue_name=queue_name)

        time = await self._client.time()
        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)
        key_hash = compat.queue_hash(self._ns, queue_name)
        pipeline = self._client.pipeline()

        pipeline.hmget(
            key_hash,
            compat.VT,
            compat.DELAY,
            compat.MAX_SIZE,
            compat.TOTAL_RECV,
            compat.TOTAL_SENT,
            compat.CREATED,
            compat.MODIFIED,
        )
        pipeline.zcard(key_sorted_set)

        # NOTE: The JavaScript implementation uses only `time[0] * 1000`, which
        # implies that sending a message and then retrieving queue attributes
        # within the same second might yield incorrect results.
        # Using `time[0] * 1000 + time[1] // 1000` would be ideal, but I will
        # stick to the original implementation.
        pipeline.zcount(key_sorted_set, time[0] * 1000, "+inf")

        result = await pipeline.execute()
        if result[0][0] is None:
            raise exceptions.QueueNotFoundException(
                f"Queue '{queue_name}' does not exist."
            )

        return QueueAttributes(
            vt=int(result[0][0]),
            delay=int(result[0][1]),
            max_size=int(result[0][2]),
            total_recv=int(result[0][3] or 0),
            total_sent=int(result[0][4] or 0),
            created=int(result[0][5]),
            modified=int(result[0][6]),
            messages=result[1],
            hidden_messages=result[2],
        )

    async def set_queue_attributes(
        self,
        queue_name: str,
        vt: Optional[int] = None,
        delay: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> QueueAttributes:
        """Update one or more attributes of a message queue.

        Args:
            queue_name: Name of the message queue.
            vt: New default visibility delay (in seconds) to use when receiving messages
                from the queue.
            delay: New default delay (in seconds) to apply when sending messages to the
                queue.
            max_size: New maximum message size for the queue (in bytes).

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.

        Returns:
            Object containing the queue's attributes, including the updated ones.
        """
        if vt is None and delay is None and max_size is None:
            raise exceptions.NoAttributesSpecified(
                "At least one queue attribute must be specified."
            )

        self._validate(queue_name=queue_name, vt=vt, delay=delay, max_size=max_size)

        # Check if the queue exists
        await self._get_queue_context(queue_name)

        key_hash = compat.queue_hash(self._ns, queue_name)

        time = await self._client.time()
        pipeline = self._client.pipeline()

        pipeline.hset(key_hash, compat.MODIFIED, time[0])
        attributes = {compat.VT: vt, compat.DELAY: delay, compat.MAX_SIZE: max_size}
        for k, v in attributes.items():
            if v is not None:
                pipeline.hset(key_hash, k, v)

        await pipeline.execute()

        return await self.get_queue_attributes(queue_name)

    def _message_length_bytes(self, message: Union[str, bytes]) -> int:
        return len(
            message.encode(self._client_encoding)
            if isinstance(message, str)
            else message
        )

    async def send_message(
        self, queue_name: str, message: Union[str, bytes], delay: Optional[int] = None
    ) -> str:
        """Send a message to a message queue.

        Args:
            queue_name: Name of the message queue.
            message: Contents of the message to send.
            delay: Delay to apply when sending the message (in seconds). If not
                specified, the queue's delay value will be used. The message will only
                be receivable after the delay period has elapsed.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.

        Returns:
            Unique ID of the message sent.
        """
        self._validate(queue_name=queue_name, delay=delay)

        context = await self._get_queue_context(queue_name, add_uid=True)
        delay = context.delay if delay is None else delay

        if (
            context.max_size != compat.MAX_SIZE_UNLIMITED
            and self._message_length_bytes(message) > context.max_size
        ):
            raise exceptions.InvalidValueException(
                f"The maximum message length in bytes is {context.max_size}."
            )

        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)
        key_hash = compat.queue_hash(self._ns, queue_name)

        pipeline = self._client.pipeline()

        pipeline.zadd(key_sorted_set, {context.uid: context.ts + delay * 1000})
        pipeline.hset(key_hash, context.uid, message)
        pipeline.hincrby(key_hash, compat.TOTAL_SENT, 1)

        if self._real_time:
            pipeline.zcard(key_sorted_set)

        result = await pipeline.execute()

        if self._real_time:
            await self._client.publish(compat.queue_rt(self._ns, queue_name), result[3])

        return utils.ensure(context.uid)

    @staticmethod
    def _message_from_script_result(result: scripts.MsgRecv) -> Message:
        return Message(
            message=result[1],
            id=result[0],
            fr=int(result[3]),
            rc=result[2],
            sent=compat.base36_decode(result[0][:10]) / 1000,
        )

    async def receive_message(
        self, queue_name: str, vt: Optional[int] = None
    ) -> Optional[Message]:
        """Receive a message from a message queue.

        **Note**: This method will return `None` immediately if the message queue is
        empty.

        After receiving a message and successfully processing it, make sure to call
        `delete_message` to ensure you won't receive it again in the future.

        Args:
            queue_name: Name of the message queue.
            vt: Visibility timer to use when receving the message (in seconds). If not
                specified, the queue's visiblity timer value will be used. After the
                message has been received, it will be invisible to consumers until the
                duration visiblity timer period has elapsed.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.

        Returns:
            Message received from the message queue if one was present, `None`
            otherwise.
        """
        self._validate(queue_name=queue_name, vt=vt)

        context = await self._get_queue_context(queue_name)
        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)
        vt = context.vt if vt is None else vt

        result: scripts.MsgRecv = await self._script_receive_message(
            keys=[key_sorted_set, context.ts, context.ts + vt * 1000]
        )
        if not result:
            return None

        return self._message_from_script_result(result)

    async def delete_message(self, queue_name: str, id: str) -> None:
        """Delete a message from a message queue.

        Args:
            queue_name: Name of the message queue containing the message.
            id: Message's unique ID.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.MessageNotFoundException: When the specified message does not
                exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.
        """
        self._validate(queue_name=queue_name, id=id)

        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)
        key_hash = compat.queue_hash(self._ns, queue_name)
        pipeline = self._client.pipeline()

        pipeline.zrem(key_sorted_set, id)
        pipeline.hdel(key_hash, id, compat.message_rc(id), compat.message_fr(id))

        result = await pipeline.execute()
        if result[0] == 0 or result[1] == 0:
            raise exceptions.MessageNotFoundException(
                f"Message with ID '{id}' does not exist."
            )

    async def pop_message(self, queue_name: str) -> Optional[Message]:
        """Receive a message from a message queue and delete it from the queue.

        **Note**: This method will return `None` immediately if the message queue is
        empty.

        With this method, there is no need to call `delete_message` after receiving a
        message. However, if an error/exception/crash occurs in your application while
        processing the message, there is a risk of losing it forever, as it will no
        longer be stored in the message queue.

        Args:
            queue_name: Name of the message queue.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.

        Returns:
            Message received from the message queue if one was present, `None`
            otherwise.
        """
        self._validate(queue_name=queue_name)

        context = await self._get_queue_context(queue_name)
        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)

        result: scripts.MsgRecv = await self._script_pop_message(
            keys=[key_sorted_set, context.ts]
        )
        if not result:
            return None

        return self._message_from_script_result(result)

    async def change_message_visibility(
        self, queue_name: str, id: str, vt: int
    ) -> None:
        """Change the visibility timer of a message.

        Args:
            queue_name: Name of the message queue containing the message.
            id: Message's unique ID.
            vt: New visibility timer value to set (in seconds). The message will be
                invisible to consumers until the visibility timer period has elapsed,
                starting from the moment this method was called.

        Raises:
            exceptions.QueueNotFoundException: When the specified queue does not exist.
            exceptions.MessageNotFoundException: When the specified message does not
                exist.
            exceptions.InvalidValueException: When a given argument contains an invalid
                value.
        """
        self._validate(queue_name=queue_name, vt=vt, id=id)

        context = await self._get_queue_context(queue_name)
        key_sorted_set = compat.queue_sorted_set(self._ns, queue_name)

        result: scripts.MsgVisibility = await self._script_change_message_visibility(
            keys=[key_sorted_set, id, context.ts + vt * 1000]
        )

        if result == 0:
            raise exceptions.MessageNotFoundException(
                f"Message with ID '{id}' does not exist."
            )

    async def quit(self) -> None:
        """Close the connection to the Redis server.

        Internally, this methods just calls the `close` method of the Redis
        client object specified in the initializator.
        """
        await self._client.close()
