import os
import re
from typing import Dict, List, Optional
from langchain.callbacks import get_openai_callback
from langchain.chains import LLMChain
from langchain.output_parsers import OutputFixingParser, PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema import OutputParserException
from langchain_core.pydantic_v1 import BaseModel
from langchain_core.language_models.base import BaseLanguageModel


class BasePrompt:
    format: BaseModel = BaseModel
    template: str = ""
    format_instruction: str = "format_instruction"

    def __init__(self) -> None:
        
        ...

    def parse(self):
        ...

    def pre_check(self):
        ...
    
    def check_token(self):
        ...

    def check_prompt(self):
        ...
    