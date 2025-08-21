import os


class Container:
    @classmethod
    def is_docker(cls) -> bool:
        return os.path.exists("/.dockerenv")

    @classmethod
    def is_pod(cls) -> bool:
        return bool([i for i in os.environ.keys() if i.startswith("KUBERNETES")])
