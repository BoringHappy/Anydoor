from anydoor.utils.check import check
import pytest


def test_check():
    with pytest.raises(EnvironmentError):

        @check.env(envs=["HHHHHHH"])
        def print_env(): ...

        print_env()
