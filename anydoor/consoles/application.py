from cleo.application import Application

from .secrets import SecretCommand
from .keepass import KeePassCommand


def main() -> int:
    application = Application()
    application.add(SecretCommand())
    application.add(KeePassCommand())
    exit_code: int = application.run()
    return exit_code
