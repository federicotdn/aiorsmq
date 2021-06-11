from typing import Text
import asyncio

import pytest

from aiorsmq import AIORSMQCore
from aiorsmq.exceptions import (
    QueueExistsException,
    MessageNotFoundException,
    QueueNotFoundException,
    NoAttributesSpecified,
)

pytestmark = pytest.mark.asyncio


async def test_create_queue(core_client: AIORSMQCore, qname: Text):
    assert qname not in (await core_client.list_queues())
    await core_client.create_queue(qname)
    assert qname in (await core_client.list_queues())


async def test_create_queue_failure(core_client: AIORSMQCore, queue: Text):
    with pytest.raises(QueueExistsException):
        await core_client.create_queue(queue)


async def test_delete_queue_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.delete_queue(qname)


async def test_delete_queue(core_client: AIORSMQCore, queue: Text):
    assert queue in (await core_client.list_queues())
    await core_client.delete_queue(queue)
    assert queue not in (await core_client.list_queues())


async def test_delete_queue_clears_messages(core_client: AIORSMQCore, queue: Text):
    await core_client.send_message(queue, "foobar")
    assert (await core_client.get_queue_attributes(queue)).messages == 1

    await core_client.delete_queue(queue)
    await core_client.create_queue(queue)

    assert (await core_client.get_queue_attributes(queue)).messages == 0


async def test_send_message_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.send_message(qname, "foobar")


async def test_send_message(core_client: AIORSMQCore, queue: Text):
    uid = await core_client.send_message(queue, "foobar")
    assert len(uid) == 32

    uids = []
    for _ in range(50):
        uid = await core_client.send_message(queue, "foobar")
        uids.append(uid)

    assert len(uids) == len(set(uids))


async def test_send_message_delay(core_client: AIORSMQCore, queue: Text):
    await core_client.send_message(queue, "foobar", delay=30)
    assert not await core_client.receive_message(queue)


async def test_receive_message_empty(core_client: AIORSMQCore, queue: Text):
    msg = await core_client.receive_message(queue)
    assert msg is None


async def test_receive_message(core_client: AIORSMQCore, queue: Text):
    uid = await core_client.send_message(queue, "foobar")
    msg = await core_client.receive_message(queue)

    assert msg is not None

    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 1
    assert msg.fr > 0
    assert msg.sent > 0


async def test_receive_message_twice_vt(core_client: AIORSMQCore, queue: Text):
    uid = await core_client.send_message(queue, "foobar")
    msg = await core_client.receive_message(queue)
    assert msg is not None and msg.id == uid

    msg = await core_client.receive_message(queue)
    assert msg is None


async def test_receive_message_twice_vt_expired(core_client: AIORSMQCore, queue: Text):
    uid = await core_client.send_message(queue, "foobar")
    await core_client.receive_message(queue, vt=1)
    await asyncio.sleep(1.5)

    msg = await core_client.receive_message(queue)
    assert msg is not None
    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 2


async def test_receive_message_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.receive_message(qname)


async def test_pop_message(core_client: AIORSMQCore, qname: Text):
    await core_client.create_queue(qname, vt=0)
    uid = await core_client.send_message(qname, "foobar")
    msg = await core_client.pop_message(qname)

    assert msg is not None

    assert msg.id == uid
    assert msg.message == "foobar"
    assert msg.rc == 1
    assert msg.fr > 0
    assert msg.sent > 0

    msg = await core_client.receive_message(qname)
    assert msg is None


async def test_pop_message_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.pop_message(qname)


async def test_change_message_visiblity(core_client: AIORSMQCore, queue: Text):
    uid = await core_client.send_message(queue, "foobar")
    msg = await core_client.receive_message(queue)
    assert msg is not None

    assert (await core_client.receive_message(queue)) is None

    await core_client.change_message_visibility(queue, uid, vt=0)

    msg = await core_client.receive_message(queue)
    assert msg is not None
    assert msg.id == uid


async def test_change_message_visiblity_failure(
    core_client: AIORSMQCore, qname: Text, msg_id: Text
):
    with pytest.raises(QueueNotFoundException):
        await core_client.change_message_visibility(qname, msg_id, 10)

    await core_client.create_queue(qname)

    with pytest.raises(MessageNotFoundException):
        await core_client.change_message_visibility(qname, msg_id, 10)


async def test_change_message_visiblity_after_pop_failure(
    core_client: AIORSMQCore, qname: Text
):
    await core_client.create_queue(qname, vt=0)
    uid = await core_client.send_message(qname, "foobar")
    msg = await core_client.pop_message(qname)
    assert msg is not None

    with pytest.raises(MessageNotFoundException):
        await core_client.change_message_visibility(qname, uid, 10)


async def test_delete_message_failure(
    core_client: AIORSMQCore, qname: Text, msg_id: Text
):
    # Queue does not exist yet
    with pytest.raises(MessageNotFoundException):
        await core_client.delete_message(qname, msg_id)

    await core_client.create_queue(qname)

    # Message does not exist yet
    with pytest.raises(MessageNotFoundException):
        await core_client.delete_message(qname, msg_id)


async def test_delete_message_no_rc(core_client: AIORSMQCore, queue: Text):
    # Delete message that was never received
    uid = await core_client.send_message(queue, "foobar")
    await core_client.delete_message(queue, uid)

    assert (await core_client.get_queue_attributes(queue)).messages == 0


async def test_delete_message(core_client: AIORSMQCore, queue: Text):
    # Delete message that was received once
    uid = await core_client.send_message(queue, "foobar")
    await core_client.receive_message(queue)

    await core_client.delete_message(queue, uid)

    assert (await core_client.get_queue_attributes(queue)).messages == 0


async def test_list_queues(core_client: AIORSMQCore, qname: Text):
    queues = await core_client.list_queues()
    await core_client.create_queue(qname)

    assert sorted(queues + [qname]) == sorted(await core_client.list_queues())


async def test_get_queue_attributes_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.get_queue_attributes(qname)


async def test_get_queue_attributes_defaults(core_client: AIORSMQCore, queue: Text):
    attributes = await core_client.get_queue_attributes(queue)

    assert attributes.vt == 30
    assert attributes.delay == 0
    assert attributes.max_size == 65536
    assert attributes.total_recv == 0
    assert attributes.total_sent == 0
    assert attributes.created > 0
    assert attributes.created == attributes.modified
    assert attributes.messages == 0
    assert attributes.hidden_messages == 0


async def test_get_queue_attributes(core_client: AIORSMQCore, qname: Text):
    vt = 44
    delay = 12
    max_size = 1000
    await core_client.create_queue(qname, vt=vt, delay=delay, max_size=max_size)
    attributes = await core_client.get_queue_attributes(qname)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size


async def test_get_queue_attributes_with_traffic(core_client: AIORSMQCore, queue: Text):
    await core_client.send_message(queue, "foobar")
    await core_client.send_message(queue, "foobar2")
    await core_client.receive_message(queue)

    # Wait for more than a second - to see why, read comment in
    # `AIORSMQCore.get_queue_attributes`.
    await asyncio.sleep(1.5)

    attributes = await core_client.get_queue_attributes(queue)

    assert attributes.total_recv == 1
    assert attributes.total_sent == 2
    assert attributes.messages == 2
    assert attributes.hidden_messages == 1


async def test_set_queue_attributes_failure(core_client: AIORSMQCore, qname: Text):
    with pytest.raises(QueueNotFoundException):
        await core_client.set_queue_attributes(qname, vt=1)

    await core_client.create_queue(qname)

    with pytest.raises(NoAttributesSpecified):
        await core_client.set_queue_attributes(qname)


async def test_set_queue_attributes(core_client: AIORSMQCore, queue: Text):
    vt = 44
    delay = 12
    max_size = 1000

    attributes = await core_client.set_queue_attributes(queue, vt, delay, max_size)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size


async def test_set_queue_attributes_from_new(core_client: AIORSMQCore, qname: Text):
    vt = 44
    delay = 12
    max_size = 1000

    await core_client.create_queue(qname, vt=vt)
    await core_client.set_queue_attributes(qname, delay=delay, max_size=max_size)
    attributes = await core_client.get_queue_attributes(qname)

    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size
