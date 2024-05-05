from langchain.chains import ConversationChain
from langchain.memory import ConversationSummaryBufferMemory
from langchain_openai import ChatOpenAI
from rich import print as rprint
from ..functions import Costs
from rich.console import Console
from rich.markdown import Markdown
from rich.theme import Theme
from prompt_toolkit import prompt
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory
from tenacity import retry, stop_after_attempt, wait_exponential
import sys


class Chat:
    cost = Costs()

    def __init__(self, mode="gpt-3.5-turbo", verbose=False) -> None:
        llm = ChatOpenAI(
            temperature=0,
            model=mode,
        )

        self.conversation = ConversationChain(
            llm=llm,
            verbose=verbose,
            memory=ConversationSummaryBufferMemory(llm=llm, max_token_limit=1000),
        )
        self.responses = []
        self.console = Console(theme=Theme({"markdown.h1": "bold"}))
        self.history = InMemoryHistory()

    def print(self, *args, **kwargs):
        self.console.print(*args, **kwargs)

    def exit(self):
        rprint("[green]Conversation end[green]")
        sys.exit(0)

    def add_file(self, human_input):
        file_path = human_input.replace("$file:", "").strip()
        self.print(f"[green]Loaded[/green]: {file_path}")

    def save(self, human_input):
        file_path = human_input.replace("$save:", "").strip()
        self.print(f"[green]Save to[/green]: {file_path}")

    def cmd(self, human_input):
        if human_input.startswith("$exit"):
            self.exit()
        elif human_input.startswith("$cost"):
            rprint(self.cost.costs)
        elif human_input.startswith("$file:"):
            self.add_file(human_input)
        elif human_input.startswith("$save:"):
            self.save(human_input)
        else:
            rprint("[red]Nothing Happen[red]")

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1.3, min=2, max=20),
    )
    @cost.openai
    def chat(self, human_input="Hi there!"):
        response = self.conversation.invoke(input=human_input)
        self.responses.append(response)
        return response

    def chat_cli(self):
        while True:
            try:
                human_input = prompt(
                    HTML("<ansiblue>You</ansiblue>: \n"),
                    history=self.history,
                )
            except KeyboardInterrupt:
                self.exit()

            if human_input.startswith("$"):
                self.cmd(human_input)
                continue
            else:
                self.print("GPT:", style="magenta")
                result = self.chat(human_input=human_input)
                self.print(Markdown(result.get("response")))
