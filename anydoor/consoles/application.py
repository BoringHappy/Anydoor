from cleo.application import Application

from .chat import ChatCommand


def main() -> int:
    application = Application()
    application.add(ChatCommand())
    exit_code: int = application.run()
    return exit_code
