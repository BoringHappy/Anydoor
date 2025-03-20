from anydoor.utils.proxy import Proxy


def test_proxy():
    assert isinstance(Proxy(), dict)
    assert "http" in Proxy()
    assert "https" in Proxy()
