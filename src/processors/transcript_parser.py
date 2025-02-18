import re
from re import Pattern

import structlog
from bs4 import BeautifulSoup
from pydantic import BaseModel
from redis.asyncio import Redis

logger = structlog.get_logger()


class TranscriptSegment(BaseModel):
    speaker: str
    transcript: str


class TranscriptParser:
    content: str
    redis_client: Redis
    soup: BeautifulSoup
    speaker_re: Pattern
    parenthesis_re: Pattern
    unclosed_re: Pattern

    def __init__(self, xhtml: str, redis_client: Redis) -> None:
        self.content = xhtml
        self.soup = BeautifulSoup(self.content, "html.parser")

        self.speaker_re = re.compile(r"^([A-Za-zÀ-ž]+,\s[A-Za-zÀ-ž]+,?\s.+)$")

        self.parenthesis_re = re.compile(r"\s*\([^)]*\)")

        self.unclosed_re = re.compile(r"\s*\([^)]*\.")
        self.client = redis_client

    async def parse(self) -> list[dict]:
        result = []
        current_speaker = None
        current_text = []
        for p in self.soup.find_all("p"):
            b_tag = p.find("b")
            speaker_extract = (
                self.speaker_re.match(b_tag.get_text(strip=True)) if b_tag else None
            )
            if b_tag and speaker_extract:
                result.append(
                    TranscriptSegment(
                        speaker=str(current_speaker),
                        transcript=" ".join(current_text).strip(),
                    )
                )
                current_speaker = b_tag.get_text(strip=True)
                current_text = []

            else:
                if b_tag and not speaker_extract:
                    logger.warning(f"Bold without speaker: {b_tag.get_text()}")

                if current_speaker:
                    p_text = p.get_text(separator=" ", strip=True)

                    # if self.speaker_re.match(p_text):
                    #     logger.warning(f"Speaker found in transcription: {p_text}")

                    if p_text:
                        p_text = self.parenthesis_re.sub("", p_text)
                        p_text = self.unclosed_re.sub("", p_text)
                        current_text.append(p_text)

        if current_speaker:
            result.append(
                TranscriptSegment(
                    speaker=current_speaker, transcript=" ".join(current_text).strip()
                )
            )

        return [i.model_dump() for i in result]

    async def extract_speaker(self, line: str):
        self.client.sismember("members", *line.split())
