from .base import BasePrompt
from langchain_core.pydantic_v1 import BaseModel, Field, validator


class Joke(BaseModel):
    setup: str = Field(description="question to set up a joke")
    punchline: str = Field(description="answer to resolve the joke")

    @validator("setup")
    def question_ends_with_question_mark(cls, field):
        if field[-1] != "?":
            raise ValueError("Badly formed question!")
        return field


class JokeCreater(BasePrompt):
    output_format = Joke
    template = "Answer the user query.\n{format_instructions}\n{query}\n"
    input_variables = ["query"]
