from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory
from langchain_openai import OpenAI
from langchain.prompts.prompt import PromptTemplate
from rich import print as rprint


class Conversation:

    def __init__(self) -> None:
        llm = OpenAI(temperature=0)

        template = (
            "The following is a friendly conversation between a human and an AI. "
            "The AI is talkative and provides lots of specific details from its context. "
            "If the AI does not know the answer to a question, it truthfully says it does not know.\n"
            "Current conversation:\n"
            "{history}\n"
            "Human: {input}\n"
            "AI Assistant:"
        )
        PROMPT = PromptTemplate(input_variables=["history", "input"], template=template)

        self.conversation = ConversationChain(
            prompt=PROMPT,
            llm=llm,
            verbose=True,
            memory=ConversationBufferMemory(),
        )

    def add(self): ...

    def chat(self, human_input="Hi there!"):
        self.conversation.predict(input=human_input)
