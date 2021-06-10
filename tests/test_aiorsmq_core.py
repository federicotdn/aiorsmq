from typing import Text

import pytest

from aiorsmq import AIORSMQCore
from aiorsmq.exceptions import QueueExistsException

pytestmark = pytest.mark.asyncio


async def test_create_queue(core_client: AIORSMQCore, qname: Text):
    await core_client.create_queue(qname)

    with pytest.raises(QueueExistsException):
        await core_client.create_queue(qname)
