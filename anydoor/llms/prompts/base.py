from typing import Dict, List, Optional
from langchain.chains import LLMChain
from langchain.output_parsers import OutputFixingParser, PydanticOutputParser
from langchain.prompts import PromptTemplate
from langchain.schema import OutputParserException
from langchain_core.pydantic_v1 import BaseModel
from langchain_core.language_models.base import BaseLanguageModel
from langchain_openai import ChatOpenAI
from ..functions import Costs

cost = Costs()


class BasePrompt:
    output_format: BaseModel = BaseModel
    template: str = ""
    format_instructions: str = "format_instructions"
    input_variables: List[str] = []

    def __init__(
        self,
        temperature=0,
        openai_model="gpt-3.5-turbo",
        openai_model_args=dict(),
        input_llm: Optional[BaseLanguageModel] = None,
        verbose=True,
    ) -> None:

        self.llm = None
        self._init_llm(input_llm, openai_model, openai_model_args, temperature)

        self.output_parser = PydanticOutputParser(pydantic_object=self.output_format)
        self.fix_parser = OutputFixingParser.from_llm(
            parser=self.output_parser, llm=self.llm
        )
        self.prompt = PromptTemplate(
            template=self.template,
            input_variables=self.input_variables,
            partial_variables={
                self.format_instructions: self.output_parser.get_format_instructions()
            },
        )
        self.chain = LLMChain(prompt=self.prompt, llm=self.llm, verbose=verbose)

    def _init_llm(
        self, input_llm=None, openai_model=None, openai_model_args=None, temperature=0
    ):
        if input_llm:
            self.llm = input_llm
        else:
            self.llm = ChatOpenAI(
                model=openai_model,
                temperature=temperature,
                **openai_model_args,
            )

    @cost.openai
    def parse(self):
        try:
            return self.output_parser.invoke(self.result["text"])
        except OutputParserException:
            return self.fix_parser.invoke(self.result["text"])
        except Exception as e:
            raise e

    def pre_check(self): ...

    def check_token(self): ...

    def check_prompt(self): ...

    @cost.openai
    def run(self, inputs: Dict, return_result=True):
        self.result = self.chain.invoke(inputs)
        if return_result:
            return self.result
