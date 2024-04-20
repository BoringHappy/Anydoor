import os


class Proxy:
    @staticmethod
    def init_in_env(host="127.0.0.1", port="7890"):
        os.environ["http_proxy"] = f"http://{host}:{port}"
        os.environ["https_proxy"] = f"http://{host}:{port}"
        os.environ["all_proxy"] = f"socks5://{host}:{port}"

    @staticmethod
    def get_proxies(host="127.0.0.1", port="7890"):
        return {
            "http": f"http://{host}:{port}",
            "https": f"http://{host}:{port}",
        }
