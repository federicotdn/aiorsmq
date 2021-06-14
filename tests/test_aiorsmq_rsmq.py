from typing import Text
import uuid

import pytest

from aiorsmq import AIORSMQ

from tests.conftest import JSClient  # type: ignore

pytestmark = pytest.mark.asyncio


async def test_receive_message_from_rsmq(
    client: AIORSMQ, js_client: JSClient, queue: Text
):
    message = uuid.uuid4().hex

    uid = js_client.send_message(queue, message, delay=0)

    received = await client.receive_message(queue)
    assert received
    assert received.message == message
    assert received.id == uid
