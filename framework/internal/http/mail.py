import httpx



class MailAPI:

    def __init__(self, base_url: str="http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def register_user(self, query: str) -> httpx.Response:
        params = {
            "query": query,
            "limit": 1,
            "kind": "containing",
            "start": 0,
        }
        return self._client.post("/mail/mail/search", params=params)