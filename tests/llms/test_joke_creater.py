from anydoor.llms.prompts import JokeCreater
from anydoor.llms.functions import Costs

print(JokeCreater().run("Tell me a joke"))
cost = Costs()
print(cost.costs)
