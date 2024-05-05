from cleo.commands.command import Command
from cleo.helpers import argument, option
from ..llms.conversation import Chat


class ChatCommand(Command):
    name = "chat"
    description = "Chat with llm"

    options = [
        option(
            "model",
            description="model name",
            default=None,
            flag=False,
        ),
    ]

    def handle(self):
        model = self.option("model")
        chat = Chat()
        chat.chat_cli()
