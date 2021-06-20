import uuid

import pytest

from aiorsmq import AIORSMQ

from tests.conftest import JSClient  # type: ignore

pytestmark = pytest.mark.asyncio


async def test_receive_messages_from_rsmq(
    client: AIORSMQ, js_client: JSClient, queue: str
):
    count = 3

    messages = []
    ids = []
    for _ in range(count):
        message = uuid.uuid4().hex
        uid = js_client.send_message(queue, message, delay=0)

        messages.append(message)
        ids.append(uid)

    for i in range(count):
        received = await client.receive_message(queue)
        assert received
        assert received.contents == messages[i]
        assert received.id == ids[i]


async def test_receive_messages_from_rsmq_delay(
    client: AIORSMQ, js_client: JSClient, queue: str
):
    js_client.send_message(queue, "foobar", delay=10)
    received = await client.receive_message(queue)
    assert received is None


async def test_interact_queue_created_with_rsmq(
    client: AIORSMQ, js_client: JSClient, qname: str
):
    delay = 0
    vt = 15
    max_size = 1024
    js_client.create_queue(qname, vt=vt, delay=delay, max_size=max_size)

    queues = await client.list_queues()
    assert queues == [qname]

    attributes = await client.get_queue_attributes(qname)
    assert attributes.vt == vt
    assert attributes.delay == delay
    assert attributes.max_size == max_size

    uid = await client.send_message(qname, "foobar")
    assert uid


async def test_receive_message_with_rsmq(
    client: AIORSMQ, js_client: JSClient, queue: str
):
    message = uuid.uuid4().hex
    uid = await client.send_message(queue, message)

    received = js_client.receive_message(queue, vt=0)
    assert received
    assert received["id"] == uid
    assert received["message"] == message
    assert received["rc"] == 1
    assert received["fr"] > received["sent"]

    # Hide the message
    received = js_client.receive_message(queue, vt=30)
    assert received

    received = await client.receive_message(queue)
    assert received is None


async def test_delete_message_with_rsmq(
    client: AIORSMQ, js_client: JSClient, queue: str
):
    # Message does not exist
    assert not js_client.delete_message(queue, "dhoiwpiirm15ce77305a5c3a3b0f230c")

    uid = await client.send_message(queue, "foobar")

    assert js_client.delete_message(queue, uid)

    # We should not be able to receive the message now

    message = await client.receive_message(queue)
    assert message is None
