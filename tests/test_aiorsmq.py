import asyncio

import pytest
import aioredis  # type: ignore

from aiorsmq import AIORSMQ, compat, utils
from aiorsmq.exceptions import (
    QueueExistsException,
    MessageNotFoundException,
    QueueNotFoundException,
    NoAttributesSpecified,
    InvalidValueException,
)

from tests.conftest import TEST_NS  # type: ignore

pytestmark = pytest.mark.asyncio


async def test_create_queue(client: AIORSMQ, qname: str):
    assert qname not in (await client.list_queues())
    await client.create_queue(qname)
    assert qname in (await client.list_queues())


async def test_create_queue_failure(client: AIORSMQ, queue: str):
    with pytest.raises(QueueExistsException):
        await client.create_queue(queue)


@pytest.mark.parametrize("name", ["", "a" * 200, "hello!"])
async def test_create_queue_failure_arg_name(client: AIORSMQ, name: str):
    with pytest.raises(InvalidValueException):
        await client.create_queue(name)


@pytest.mark.parametrize("delay", [-1, 10000000000000000])
async def test_create_queue_failure_arg_delay(client: AIORSMQ, qname: str, delay: int):
    with pytest.raises(InvalidValueException):
        await client.create_queue(qname, delay=delay)


@pytest.mark.parametrize("max_size", [-2, 100, 10000000000000000])
async def test_create_queue_failure_arg_max_size(
    client: AIORSMQ, qname: str, max_size: int
):
    with pytest.raises(InvalidValueException):
        await client.create_queue(qname, max_size=max_size)


async def test_delete_queue_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.delete_queue(qname)


@pytest.mark.parametrize("name", ["", "a" * 200, "hello!"])
async def test_delete_queue_failure_arg(client: AIORSMQ, name: str):
    with pytest.raises(InvalidValueException):
        await client.delete_queue(name)


async def test_delete_queue(client: AIORSMQ, queue: str):
    assert queue in (await client.list_queues())
    await client.delete_queue(queue)
    assert queue not in (await client.list_queues())


async def test_delete_queue_clears_messages(client: AIORSMQ, queue: str):
    await client.send_message(queue, "foobar")
    assert (await client.get_queue_attributes(queue)).messages == 1

    await client.delete_queue(queue)
    await client.create_queue(queue)

    assert (await client.get_queue_attributes(queue)).messages == 0


async def test_send_message_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.send_message(qname, "foobar")


async def test_send_message_failure_max_size(client: AIORSMQ, queue: str):
    await client.set_queue_attributes(queue, max_size=1024)

    message = "a" * 1025

    with pytest.raises(InvalidValueException):
        await client.send_message(queue, message)


async def test_send_message(client: AIORSMQ, queue: str):
    uid = await client.send_message(queue, "foobar")
    assert len(uid) == 32

    uids = []
    for _ in range(50):
        uid = await client.send_message(queue, "foobar")
        uids.append(uid)

    assert len(uids) == len(set(uids))


async def test_send_message_rt(
    redis_client: aioredis.Redis, client: AIORSMQ, queue: str
):
    pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
    rt_key = compat.queue_rt(TEST_NS, queue)
    await pubsub.subscribe(rt_key)

    await client.send_message(queue, "foobar")

    value = None
    while not value:
        value = await pubsub.get_message()

    assert value["channel"] == rt_key
    assert value["data"] == "1"

    await client.send_message(queue, "foobar2")

    value = None
    while not value:
        value = await pubsub.get_message()

    assert value["data"] == "2"

    await pubsub.unsubscribe(rt_key)
    await pubsub.close()


async def test_send_message_delay(client: AIORSMQ, queue: str):
    await client.send_message(queue, "foobar", delay=30)
    assert not await client.receive_message(queue)


async def test_send_message_delay_queue_configured(client: AIORSMQ, qname: str):
    await client.create_queue(qname, delay=30)
    await client.send_message(qname, "foobar")
    assert not await client.receive_message(qname)


async def test_receive_message_empty(client: AIORSMQ, queue: str):
    msg = await client.receive_message(queue)
    assert msg is None


async def test_receive_message(client: AIORSMQ, queue: str):
    uid = await client.send_message(queue, "foobar")
    msg = await client.receive_message(queue)

    assert msg is not None

    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 1
    assert msg.fr > 0
    assert msg.sent > 0


async def test_receive_message_sent_order(client: AIORSMQ, queue: str):
    count = 3

    for _ in range(count):
        await client.send_message(queue, "foobar")

    messages = [await client.receive_message(queue) for _ in range(count)]

    for i in range(count - 1):
        assert utils.ensure(messages[i]).sent < utils.ensure(messages[i + 1]).sent


async def test_receive_message_twice_vt(client: AIORSMQ, queue: str):
    uid = await client.send_message(queue, "foobar")
    msg = await client.receive_message(queue)
    assert msg is not None and msg.id == uid

    msg = await client.receive_message(queue)
    assert msg is None


async def test_receive_message_twice_vt_expired(client: AIORSMQ, queue: str):
    uid = await client.send_message(queue, "foobar")
    await client.receive_message(queue, vt=0)

    msg = await client.receive_message(queue)
    assert msg is not None
    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 2


async def test_receive_message_twice_vt_expired_queue_configured(
    client: AIORSMQ, qname: str
):
    await client.create_queue(qname, vt=0)
    uid = await client.send_message(qname, "foobar")
    await client.receive_message(qname)

    msg = await client.receive_message(qname)
    assert msg is not None
    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 2


async def test_receive_message_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.receive_message(qname)


@pytest.mark.parametrize("vt", [-1, 10000000000000])
async def test_receive_message_failure_arg(client: AIORSMQ, queue: str, vt: int):
    with pytest.raises(InvalidValueException):
        await client.receive_message(queue, vt)


async def test_pop_message_fifo_order(client: AIORSMQ, queue: str):
    messages = [str(i) for i in range(100)]
    for m in messages:
        await client.send_message(queue, m)

    for m in messages:
        received = await client.pop_message(queue)
        assert received is not None
        assert received.message == m


async def test_pop_message(client: AIORSMQ, qname: str):
    await client.create_queue(qname, vt=0)
    uid = await client.send_message(qname, "foobar")
    msg = await client.pop_message(qname)

    assert msg is not None

    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 1
    assert msg.fr > 0
    assert msg.sent > 0

    msg = await client.receive_message(qname)
    assert msg is None


async def test_pop_message_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.pop_message(qname)


async def test_change_message_visiblity(client: AIORSMQ, queue: str):
    uid = await client.send_message(queue, "foobar")
    msg = await client.receive_message(queue)
    assert msg is not None

    assert (await client.receive_message(queue)) is None

    await client.change_message_visibility(queue, uid, vt=0)

    msg = await client.receive_message(queue)
    assert msg is not None
    assert msg.id == uid


async def test_change_message_visiblity_failure(
    client: AIORSMQ, qname: str, msg_id: str
):
    with pytest.raises(QueueNotFoundException):
        await client.change_message_visibility(qname, msg_id, 10)

    await client.create_queue(qname)

    with pytest.raises(MessageNotFoundException):
        await client.change_message_visibility(qname, msg_id, 10)


async def test_change_message_visiblity_after_pop_failure(client: AIORSMQ, qname: str):
    await client.create_queue(qname, vt=0)
    uid = await client.send_message(qname, "foobar")
    msg = await client.pop_message(qname)
    assert msg is not None

    with pytest.raises(MessageNotFoundException):
        await client.change_message_visibility(qname, uid, 10)


async def test_delete_message_failure(client: AIORSMQ, qname: str, msg_id: str):
    # Queue does not exist yet
    with pytest.raises(MessageNotFoundException):
        await client.delete_message(qname, msg_id)

    await client.create_queue(qname)

    # Message does not exist yet
    with pytest.raises(MessageNotFoundException):
        await client.delete_message(qname, msg_id)


async def test_delete_message_no_rc(client: AIORSMQ, queue: str):
    # Delete message that was never received
    uid = await client.send_message(queue, "foobar")
    await client.delete_message(queue, uid)

    assert (await client.get_queue_attributes(queue)).messages == 0


async def test_delete_message(client: AIORSMQ, queue: str):
    # Delete message that was received once
    uid = await client.send_message(queue, "foobar")
    await client.receive_message(queue)

    await client.delete_message(queue, uid)

    assert (await client.get_queue_attributes(queue)).messages == 0


@pytest.mark.parametrize("id", ["", "testing", "!" * 32])
async def test_delete_message_failure_arg(client: AIORSMQ, queue: str, id: str):
    with pytest.raises(InvalidValueException):
        await client.delete_message(queue, id)


async def test_list_queues(client: AIORSMQ, qname: str):
    queues = await client.list_queues()
    assert queues == []

    await client.create_queue(qname)
    assert await client.list_queues() == [qname]


async def test_get_queue_attributes_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.get_queue_attributes(qname)


async def test_get_queue_attributes_defaults(client: AIORSMQ, queue: str):
    attributes = await client.get_queue_attributes(queue)

    assert attributes.vt == 30
    assert attributes.delay == 0
    assert attributes.max_size == 65536
    assert attributes.total_recv == 0
    assert attributes.total_sent == 0
    assert attributes.created > 0
    assert attributes.created == attributes.modified
    assert attributes.messages == 0
    assert attributes.hidden_messages == 0


async def test_get_queue_attributes(client: AIORSMQ, qname: str):
    vt = 44
    delay = 12
    max_size = 2000
    await client.create_queue(qname, vt=vt, delay=delay, max_size=max_size)
    attributes = await client.get_queue_attributes(qname)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size


async def test_get_queue_attributes_with_traffic(client: AIORSMQ, queue: str):
    await client.send_message(queue, "foobar")
    await client.send_message(queue, "foobar2")
    await client.receive_message(queue)

    # Wait for more than a second - to see why, read comment in
    # `AIORSMQ.get_queue_attributes`.
    await asyncio.sleep(1.5)

    attributes = await client.get_queue_attributes(queue)

    assert attributes.total_recv == 1
    assert attributes.total_sent == 2
    assert attributes.messages == 2
    assert attributes.hidden_messages == 1


async def test_set_queue_attributes_failure(client: AIORSMQ, qname: str):
    with pytest.raises(QueueNotFoundException):
        await client.set_queue_attributes(qname, vt=1)

    await client.create_queue(qname)

    with pytest.raises(NoAttributesSpecified):
        await client.set_queue_attributes(qname)


async def test_set_queue_attributes(client: AIORSMQ, queue: str):
    vt = 44
    delay = 12
    max_size = 2000

    attributes = await client.set_queue_attributes(queue, vt, delay, max_size)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size


async def test_set_queue_attributes_from_new(client: AIORSMQ, qname: str):
    vt = 44
    delay = 12
    max_size = 2000

    await client.create_queue(qname, vt=vt)
    await client.set_queue_attributes(qname, delay=delay, max_size=max_size)
    attributes = await client.get_queue_attributes(qname)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size


async def test_quit(client: AIORSMQ):
    # Should not raise
    await client.quit()
