from typing import Text
import random

import pytest

from aiorsmq import compat


@pytest.mark.parametrize(
    "n, expected", [(0, "0"), (1623361341927, "kprffg13"), (1, "1")]
)
def test_base36_encode(n: int, expected: Text):
    assert compat.base36_encode(n) == expected


@pytest.mark.parametrize("val, expected", [("kprffg13", 1623361341927), ("0", 0)])
def test_base36_decode(val: Text, expected: int):
    assert compat.base36_decode(val) == expected


def test_base36():
    for _ in range(100):
        n = random.randint(0, 10000000000000)
        encoded = compat.base36_encode(n)
        decoded = compat.base36_decode(encoded)

        assert decoded == n


def test_make_id():
    assert len(compat.make_id()) == 22
    assert compat.make_id() != compat.make_id()


@pytest.mark.parametrize(
    "n, expected",
    [
        (0, "000000"),
        (123, "000123"),
        (9999, "009999"),
        (123456, "123456"),
        (1111111, "1111111"),
    ],
)
def test_format_zero_pad(n: int, expected: Text):
    assert compat.format_zero_pad(n, 6) == expected


def test_queue_hash():
    assert compat.queue_hash("test", "foo") == "test:foo:Q"


def test_queue_sorted_set():
    assert compat.queue_sorted_set("test", "foo") == "test:foo"


def test_queues_set():
    assert compat.queues_set("test") == "test:QUEUES"


def test_queue_rt():
    assert compat.queue_rt("test", "foo") == "test:rt:foo"
