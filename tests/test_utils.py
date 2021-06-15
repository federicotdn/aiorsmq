from aiorsmq import utils

import pytest


def test_ensure():
    with pytest.raises(RuntimeError):
        utils.ensure(None)

    assert utils.ensure(1) == 1
