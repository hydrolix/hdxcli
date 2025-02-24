class RcloneAPIConfig:
    def __init__(
        self,
        host: str,
        user: str = None,
        password: str = None,
        port: str = "5572",
        scheme: str = "http",
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.scheme = scheme

    def get_url(self):
        return f"{self.scheme}://{self.host}:{self.port}"
