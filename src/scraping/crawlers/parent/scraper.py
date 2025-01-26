from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator


class Scraper(ABC):
    url: str

    @abstractmethod
    def __init__(self, url: str): ...

    @abstractmethod
    async def scrape(self, **kwargs) -> AsyncGenerator[Any, None]: ...
