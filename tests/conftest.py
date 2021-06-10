from typing import Text
import random
import string

import pytest
import aioredis  # type: ignore
from aioredis.client import Redis  # type: ignore

from aiorsmq import AIORSMQCore

TEST_NS = "testing"


@pytest.fixture(scope="session")
def redis_client() -> Redis:
    return aioredis.from_url(
        "redis://localhost", encoding="utf-8", decode_responses=True
    )


@pytest.fixture
def core_client(redis_client: aioredis.client.Redis) -> AIORSMQCore:
    return AIORSMQCore(client=redis_client, ns=TEST_NS, real_time=True)


@pytest.fixture
def qname() -> Text:
    return "".join([random.choice(string.ascii_lowercase) for _ in range(6)])
