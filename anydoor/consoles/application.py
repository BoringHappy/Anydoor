from cleo.application import Application

from .secret_command import SecretCommand


def main() -> int:
    application = Application()
    application.add(SecretCommand())
    exit_code: int = application.run()
    return exit_code
