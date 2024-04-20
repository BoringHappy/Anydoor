import os


class Container:

    @classmethod
    def is_docker(cls):
        return os.path.exists("/.dockerenv")

    @classmethod
    def is_pod(cls):
        return bool([i for i in os.environ.keys() if i.startswith("KUBERNETES")])
