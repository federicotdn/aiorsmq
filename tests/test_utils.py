from aiorsmq import utils

import pytest


def test_unrwap():
    with pytest.raises(RuntimeError):
        utils.unwrap(None)

    assert utils.unwrap(1) == 1
