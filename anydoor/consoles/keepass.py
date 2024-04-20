from cleo.commands.command import Command
from cleo.helpers import argument, option
from anydoor.utils import KeePass


class KeePassCommand(Command):
    name = "keepass"
    description = "KeePass Operations"
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
            "password",
            description="secret current json value",
            default=None,
            flag=False,
        ),
    ]

    def handle(self):
        action = self.argument("action")
        if not action:
            self.line("<info>Action is need</info>")

        keepass = KeePass()
        self.line("<info>Success</info>")
