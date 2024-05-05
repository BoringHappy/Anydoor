from langchain_community.callbacks import get_openai_callback
from ...utils import SingletonType


class Costs(metaclass=SingletonType):

    def __init__(self) -> None:
        self.costs = []

    def openai(self, func):
        def wrapper(*args, **kwargs):
            # Assuming get_openai_callback is a method that fetches cost
            with get_openai_callback() as cost:
                result = func(*args, **kwargs)
                self.costs.append(cost)
            return result

        return wrapper
