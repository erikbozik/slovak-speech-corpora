import asyncio
from typing import AsyncGenerator

from aiohttp import ClientSession
from sqlalchemy import and_, select

from sqlalchemy.ext.asyncio import AsyncSession
from tqdm import tqdm

from src.database import NRSRTranscript


class TikaRunner:
    url: str
    session: AsyncSession
    client: ClientSession

    def __init__(
        self, tika_url: str, session: AsyncSession, client: ClientSession
    ) -> None:
        self.url = tika_url
        self.session = session
        self.client = client
        self.offset = 0

    async def fetch_db(self, n: int) -> AsyncGenerator[NRSRTranscript, None]:
        result = await self.session.execute(
            select(NRSRTranscript)
            .where(
                and_(
                    NRSRTranscript.xhtml_parsed == None,  # noqa
                    NRSRTranscript.scraped_file_type == "docx",
                )
            )
            .limit(n)
        )

        for i in result.scalars():
            yield i

    async def call_tika(self, transcript: NRSRTranscript):
        async with self.client.put(self.url, data=transcript.scraped_file) as response:
            if response.status != 200:
                raise Exception(f"Tika failed for id {transcript.id}")
            parsed_text = await response.text()

        transcript.xhtml_parsed = parsed_text  # type: ignore

    async def run_tasks(self, tasks: list):
        async with asyncio.TaskGroup() as group:
            for task in tasks:
                group.create_task(task)

    async def run_tika(self, parallel: int = 20, total: int = 2111):
        tasks = [self.call_tika(i) async for i in self.fetch_db(parallel)]
        bar = tqdm(total=total)
        while tasks:
            await self.run_tasks(tasks)
            await self.session.commit()

            bar.update(len(tasks))
            tasks = [self.call_tika(i) async for i in self.fetch_db(parallel)]

        bar.close()
