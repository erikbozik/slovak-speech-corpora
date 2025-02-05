from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator

from src.scraping.link_queue import URLRecord


class Scraper(ABC):
    url: str

    @abstractmethod
    def __init__(self, data: URLRecord): ...

    @abstractmethod
    async def scrape(self, **kwargs) -> AsyncGenerator[Any, None]: ...

    @abstractmethod
    async def save(self, item: Any, **kwargs) -> None: ...
