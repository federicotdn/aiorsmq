from typing import AsyncGenerator, Dict, Any
import random
import string
import time
import json
import subprocess
from pathlib import Path
from datetime import datetime

import pytest
import aioredis  # type: ignore
from aiorsmq import AIORSMQ, compat

HOST = "localhost"
PORT = 6379
TEST_NS = "testing"
JS_DIR = Path(__file__).parent.resolve() / "js"
JS_HELPER = JS_DIR / "helper.js"


class JSClient:
    def __init__(
        self, *, host: str, port: int, namespace: str, real_time: bool
    ) -> None:
        self._host = host
        self._port = port
        self._namespace = namespace
        self._real_time = real_time

    def _run(self, method: str, args: Dict[str, Any]) -> Dict[str, Any]:
        input_data = {
            "host": self._host,
            "port": self._port,
            "namespace": self._namespace,
            "real_time": self._real_time,
            "method": method,
        }

        input_data = {**input_data, **args}

        result = subprocess.run(
            f"node {JS_HELPER}",
            shell=True,
            cwd=JS_DIR,
            input=json.dumps(input_data),
            encoding="utf-8",
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
        )

        if result.returncode:
            raise Exception(f"Error received from JS client:\n{result.stdout}")

        return json.loads(result.stdout)

    def send_message(
        self, queue_name: str, message: str, delay: int = compat.DEFAULT_DELAY
    ) -> str:
        result = self._run(
            "send_message", {"qname": queue_name, "message": message, "delay": delay}
        )

        return result["id"]

    def create_queue(
        self,
        queue_name: str,
        vt: int = compat.DEFAULT_VT,
        delay: int = compat.DEFAULT_DELAY,
        max_size: int = compat.DEFAULT_MAX_SIZE,
    ) -> None:
        self._run(
            "create_queue",
            {"qname": queue_name, "vt": vt, "delay": delay, "maxsize": max_size},
        )

    def receive_message(
        self, queue_name: str, vt: int = compat.DEFAULT_VT
    ) -> Dict[str, Any]:
        return self._run("receive_message", {"qname": queue_name, "vt": vt})

    def delete_message(self, queue_name: str, id: str) -> bool:
        result = self._run("delete_message", {"qname": queue_name, "id": id})
        return bool(result["deleted"])


@pytest.fixture()
def js_client() -> JSClient:
    return JSClient(host=HOST, port=PORT, namespace=TEST_NS, real_time=True)


@pytest.fixture()
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    client = aioredis.from_url(
        "redis://" + HOST, encoding="utf-8", decode_responses=True
    )
    yield client

    await client.flushall()
    await client.close()


@pytest.fixture
def client(redis_client: aioredis.Redis) -> AIORSMQ:
    return AIORSMQ(client=redis_client, namespace=TEST_NS, real_time=True)


@pytest.fixture
def qname() -> str:
    return "".join([random.choice(string.ascii_lowercase) for _ in range(6)])


@pytest.fixture
def msg_id() -> str:
    unix_time = int(time.time())
    microseconds = datetime.now().microsecond
    return compat.message_uid(unix_time, microseconds)


@pytest.fixture
async def queue(client: AIORSMQ, qname: str) -> str:
    await client.create_queue(qname)
    return qname
