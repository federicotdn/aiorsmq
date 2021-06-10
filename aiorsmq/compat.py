import random
from typing import Text

DEFAULT_VT = 30
DEFAULT_DELAY = 0
DEFAULT_MAX_SIZE = 65536
DEFAULT_NAMESPACE = "rsmq"
TOTAL_SENT = "totalsent"
NAMESPACE_SEP = ":"
QUEUE_NAME_SUFFIX = "Q"
QUEUES_SUFFIX = "QUEUES"
ID_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
DEFAULT_ID_RAND_LENGTH = 22

VT = "vt"
RT = "rt"
DELAY = "delay"
MAX_SIZE = "maxsize"
CREATED = "created"
MODIFIED = "modified"

BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


def queue_name(ns: Text, base: Text, with_q: bool = True) -> Text:
    name = ns + NAMESPACE_SEP + base

    if with_q:
        name += NAMESPACE_SEP + QUEUE_NAME_SUFFIX

    return name


def queues_set(ns: Text) -> Text:
    return ns + NAMESPACE_SEP + QUEUES_SUFFIX


def queue_rt(ns: Text, base: Text) -> Text:
    return ns + NAMESPACE_SEP + RT + NAMESPACE_SEP + base


def format_zero_pad(n: int, count: int) -> Text:
    return str(n).zfill(count)


def make_id(length: int = DEFAULT_ID_RAND_LENGTH) -> Text:
    return "".join([random.choice(ID_CHARACTERS) for _ in range(length)])


def base36_encode(n: int):
    if n == 0:
        return "0"

    result = ""
    while n != 0:
        n, i = divmod(n, len(BASE36_ALPHABET))
        result = BASE36_ALPHABET[i] + result

    return result
