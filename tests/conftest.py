from typing import Text, AsyncGenerator
import random
import string
import time
from datetime import datetime

import pytest
import aioredis  # type: ignore
from aiorsmq import AIORSMQ, compat

TEST_NS = "testing"


@pytest.fixture()
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    client = aioredis.from_url(
        "redis://localhost", encoding="utf-8", decode_responses=True
    )
    yield client
    await client.close()


@pytest.fixture
def core_client(redis_client: aioredis.Redis) -> AIORSMQ:
    return AIORSMQ(client=redis_client, ns=TEST_NS, real_time=True)


@pytest.fixture
def qname() -> Text:
    return "".join([random.choice(string.ascii_lowercase) for _ in range(6)])


@pytest.fixture
def msg_id() -> Text:
    unix_time = int(time.time())
    microseconds = datetime.now().microsecond
    return compat.message_uid(unix_time, microseconds)


@pytest.fixture
async def queue(core_client: AIORSMQ, qname: Text) -> Text:
    await core_client.create_queue(qname)
    return qname
