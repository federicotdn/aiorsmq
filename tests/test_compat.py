from datetime import datetime
import random
import time

import pytest

from aiorsmq import compat


@pytest.mark.parametrize(
    "n, expected", [(0, "0"), (1623361341927, "kprffg13"), (1, "1")]
)
def test_base36_encode(n: int, expected: str):
    assert compat.base36_encode(n) == expected


@pytest.mark.parametrize("val, expected", [("kprffg13", 1623361341927), ("0", 0)])
def test_base36_decode(val: str, expected: int):
    assert compat.base36_decode(val) == expected


def test_base36():
    for _ in range(100):
        n = random.randint(0, 10000000000000)
        encoded = compat.base36_encode(n)
        decoded = compat.base36_decode(encoded)

        assert decoded == n


def test_queue_hash():
    assert compat.queue_hash("test", "foo") == "test:foo:Q"


def test_queue_sorted_set():
    assert compat.queue_sorted_set("test", "foo") == "test:foo"


def test_queues_set():
    assert compat.queues_set("test") == "test:QUEUES"


def test_queue_rt():
    assert compat.queue_rt("test", "foo") == "test:rt:foo"


def test_message_fr():
    assert compat.message_fr("foo") == "foo:fr"


def test_message_rc():
    assert compat.message_rc("foo") == "foo:rc"


def test_message_uid():
    unix_time = int(time.time())
    microseconds = datetime.now().microsecond

    uid = compat.message_uid(unix_time, microseconds)

    # Length should be constant
    assert len(uid) == 32

    for ch in uid:
        assert ch in compat.ID_CHARACTERS

    # First 10 digits should represent a timestamp in microseconds
    result = compat.base36_decode(uid[:10])
    assert result == (unix_time * 1000000) + microseconds
    assert (result // 1000) == (unix_time * 1000 + microseconds // 1000)
