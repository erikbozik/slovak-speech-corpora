import asyncio
import re
from pathlib import Path
from urllib.parse import urljoin

import aiofiles
import structlog
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import NRSRRecording
from src.extractors.utils import AudioAnalyzer

from ..link_queue import MetaData, NRSRRecordingData, URLRecord
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

        await logger.ainfo(f"Extracting {self.url}")
        result = await self.extract_audio_from_chunklist(chunklist_url)

        meeting_num = re.findall(r"(\d+)\.\s*schôdza", self.metadata.name)

        if meeting_num:
            meeting_num = int(meeting_num[0])
        else:
            await logger.aerror(f"Could not parse meeting num out of {self.url}")
            meeting_num = None
        await logger.ainfo("Adding to database")

        analyzed = AudioAnalyzer(result).analyze()

        duration, sampling_rate = analyzed.duration, analyzed.sampling_rate
        size = len(result) / 1024**2

        yield NRSRRecordingData(
            audio=result,
            metadata=NRSRRecording(
                meeting_name=self.metadata.name,
                meeting_num=meeting_num,
                snapshot=self.metadata.snapshot,
                audio_format="wav",
                audio_size=size,
                duration=duration,
                sampling_rate=sampling_rate,
            ),
        )

    async def save(self, item: NRSRRecordingData, session: AsyncSession, folder: str):
        recording_folder: Path = Path(folder)
        async with session.begin():
            session.add(item.metadata)

        filename = (
            f"{item.metadata.meeting_num}_{item.metadata.snapshot.strftime('%d-%m-%Y')}"
        )
        path = recording_folder / filename

        async with aiofiles.open(f"{path}.mp3", "wb") as file:
            await file.write(item.audio)
        await logger.ainfo(f"{filename} recording saved to the file system")

    @staticmethod
    async def extract_audio_from_chunklist(chunklist_url: str) -> bytes:
        cmd = [
            "ffmpeg",
            "-i",
            chunklist_url,
            "-vn",
            "-acodec",
            "libmp3lame",  # Use the MP3 encoder instead of pcm_s16le
            "-ar",
            "48000",
            "-ac",
            "2",
            "-b:a",
            "192k",  # Optional: set the audio bitrate
            "-f",
            "mp3",  # Set the output format to mp3
            "pipe:1",
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            await logger.aerror("ffmpeg processing failed", error=stderr.decode())
            raise Exception(f"ffmpeg process failed: {stderr.decode()}")

        return stdout

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
