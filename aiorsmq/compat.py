import random
from typing import Text

DEFAULT_VT = 30
DEFAULT_DELAY = 0
DEFAULT_MAX_SIZE = 65536
DEFAULT_NAMESPACE = "rsmq"
TOTAL_SENT = "totalsent"
TOTAL_RECV = "totalrecv"
NAMESPACE_SEP = ":"
QUEUE_HASH_SUFFIX = "Q"
QUEUES_SUFFIX = "QUEUES"
ID_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
DEFAULT_ID_RAND_LENGTH = 22

VT = "vt"
RT = "rt"
DELAY = "delay"
MAX_SIZE = "maxsize"
CREATED = "created"
MODIFIED = "modified"
RC = "rc"
FR = "fr"

BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


def _ns_join(*args: Text) -> Text:
    return NAMESPACE_SEP.join(args)


def queue_hash(ns: Text, base: Text) -> Text:
    return _ns_join(ns, base, QUEUE_HASH_SUFFIX)


def queue_sorted_set(ns: Text, base: Text) -> Text:
    return _ns_join(ns, base)


def queues_set(ns: Text) -> Text:
    return _ns_join(ns, QUEUES_SUFFIX)


def queue_rt(ns: Text, base: Text) -> Text:
    return _ns_join(ns, RT, base)


def message_rc(id: Text) -> Text:
    return _ns_join(id, RC)


def message_fr(id: Text) -> Text:
    return _ns_join(id, FR)


def message_uid(unix_time: int, microseconds: int) -> Text:
    suffix = "".join(
        [random.choice(ID_CHARACTERS) for _ in range(DEFAULT_ID_RAND_LENGTH)]
    )

    return base36_encode(unix_time * 1000000 + microseconds) + suffix


def base36_encode(n: int) -> Text:
    if n == 0:
        return "0"

    result = ""
    while n != 0:
        n, i = divmod(n, len(BASE36_ALPHABET))
        result = BASE36_ALPHABET[i] + result

    return result


def base36_decode(value: Text) -> int:
    return int(value, 36)
