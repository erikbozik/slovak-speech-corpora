import asyncio
import re
from urllib.parse import urljoin

import structlog
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession
from tqdm import tqdm

from src.database import NRSRRecording

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

        result = bytes()
        for ts_url in tqdm(self.get_ts_urls(chunklist_url, content)):
            async with client.get(ts_url) as response:
                chunk = await response.content.read()
                if not chunk:
                    await logger.aerror("Empty chunk recieved")
                    continue
                result += chunk

        meeting_num = re.findall(r"(\d+)\.\s*schôdza", self.metadata.name)

        if meeting_num:
            meeting_num = int(meeting_num[0])
        else:
            await logger.aerror(f"Could not parse meeting num out of {self.url}")
            meeting_num = None

        result = await self.convert_ts_to_mp4_bytes(result)

        yield NRSRRecording(
            meeting_name=self.metadata.name,
            meeting_num=meeting_num,
            snapshot=self.metadata.snapshot,
            video_recording=result,
            video_format="mp4",
        )

    async def save(self, item: NRSRRecording, session: AsyncSession):
        async with session.begin():
            session.add(item)
        await logger.ainfo(f"{item} added to database")

    @staticmethod
    async def convert_ts_to_mp4_bytes(ts_bytes):
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-i",
            "pipe:0",
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "frag_keyframe+empty_moov",
            "-f",
            "mp4",
            "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        mp4_bytes, error = await process.communicate(input=ts_bytes)

        if process.returncode != 0:
            raise RuntimeError(f"ffmpeg error: {error.decode()}")

        return mp4_bytes

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
