import pytest

from anydoor.utils.proxy import Proxy


@pytest.mark.skip(reason="No Proxy")
def test_proxy():
    assert isinstance(Proxy(), dict)
    assert "http" in Proxy()
    assert "https" in Proxy()
