import os


def is_docker():
    return os.path.exists("/.dockerenv")


def is_pod():
    return bool([i for i in os.environ.keys() if i.startswith("KUBERNETES")])
