from typing import TypeVar, Optional

T = TypeVar("T")


def ensure(value: Optional[T]) -> T:
    if value is None:
        raise RuntimeError("Expected a non-None value to be present.")

    return value
