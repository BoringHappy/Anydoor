from cleo.commands.command import Command
from cleo.helpers import argument, option
from ..utils import Secret, logger


class SecretCommand(Command):
    name = "secret"
    description = "Secret Operations"
    arguments = [
        argument("action", description="Action: add,generate,delete", optional=True)
    ]
    options = [
        option(
            "name",
            description="secret name",
            default=None,
            flag=False,
        ),
        option(
            "path",
            description="secret current json path",
            default=None,
            flag=False,
        ),
        option(
            "value",
            description="secret current json value",
            default=None,
            flag=False,
        ),
    ]

    def handle(self):
        action = self.argument("action")
        if action == "get":
            sec = Secret.get(self.option("name"))
            self.line(f"{sec}")
        elif action == "add":
            Secret.add(secret_name=self.option("name"), secret_path=self.option("path"))
        elif action == "list":
            Secret.list()

        self.line("Success", "info")
