from cleo.application import Application

from .secrets import SecretCommand
from .keepass import KeePassCommand
from .chat import ChatCommand


def main() -> int:
    application = Application()
    application.add(SecretCommand())
    application.add(KeePassCommand())
    application.add(ChatCommand())
    exit_code: int = application.run()
    return exit_code
