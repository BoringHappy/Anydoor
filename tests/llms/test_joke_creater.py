from anydoor.llms.prompts import JokeCreater
from anydoor.llms.functions import Costs
import pytest


@pytest.mark.skip(reason="Call OPENAI API")
def test_llm():
    print(JokeCreater(openai_model="gpt-3.5-turbo-1106").run("Tell me a joke"))
    cost = Costs()
    print(cost.costs)

if __name__ == "__main__":
    test_llm()