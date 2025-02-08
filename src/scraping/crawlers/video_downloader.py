import re
from urllib.parse import urljoin

import aiofiles
import structlog
from aiohttp import ClientSession
from tqdm import tqdm

from ..link_queue import MetaData, URLRecord
from .parent import Scraper

logger = structlog.get_logger()


class VideoDownloader(Scraper):
    url: str
    video_recording_url: str | None
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.video_recording_url = None
        self.metadata = data.metadata

    async def scrape(self, client: ClientSession):
        async with client.get(self.url) as response:
            content = await response.text()

        playlist_url = await self.parse_playlist(content)

        async with client.get(playlist_url) as response:
            content = await response.text()

        chunklist_url = await self.get_chunklist_url(playlist_url, content)

        async with client.get(chunklist_url) as response:
            content = await response.text()

        for ts_url in tqdm(self.get_ts_urls(chunklist_url, content)):
            async with client.get(ts_url) as response:
                chunk = await response.content.read()
                if not chunk:
                    await logger.aerror("Empty chunk recieved")
                    continue
                yield chunk

    async def save(self, chunk: bytes):
        async with aiofiles.open(
            self.metadata.name + str(self.metadata.snapshot), "ab"
        ) as f:
            await f.write(chunk)

    @staticmethod
    def get_ts_urls(chunklist_url: str, content: str) -> list[str]:
        ts_files = [i for i in content.split("\n") if i and not i.startswith("#")]

        return [urljoin(chunklist_url, i) for i in ts_files]

    @staticmethod
    async def get_chunklist_url(playlist_url: str, content: str) -> str:
        chunklist_name = [i for i in content.split("\n") if i and not i.startswith("#")]

        if len(chunklist_name) == 1:
            chunklist_name = chunklist_name[0]
        else:
            await logger.aerror(f"Could not parse chunklist name from {playlist_url}")
            raise ValueError()

        suffix = "/playlist.m3u8"
        if playlist_url.endswith(suffix):
            base_url = playlist_url[: -len(suffix)]
        else:
            await logger.aerror(
                f"Could not get base url out of playlist url: {playlist_url}"
            )
            raise ValueError()
        return f"{base_url}/{chunklist_name}"

    @staticmethod
    async def parse_playlist(html: str) -> str:
        pattern = r"((?:(?:https?:)?//)?[^'\"]+playlist\.m3u8)"
        matched_url = re.search(pattern, html)

        if matched_url:
            return "https:" + matched_url.group(1)
        else:
            await logger.aerror("Playlist not found on the given site")
            raise ValueError()
