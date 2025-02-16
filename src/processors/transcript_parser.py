import re
from re import Pattern
from typing import Generator

import structlog
from bs4 import BeautifulSoup
from pydantic import BaseModel

logger = structlog.get_logger()


class TranscriptSegment(BaseModel):
    speaker: str
    transcript: str


class TranscriptParser:
    content: str
    soup: BeautifulSoup
    speaker_re: Pattern
    parenthesis_re: Pattern
    unclosed_re: Pattern

    def __init__(self, xhtml: str) -> None:
        self.content = xhtml
        self.soup = BeautifulSoup(self.content, "html.parser")

        self.speaker_re = re.compile(r"^([A-Za-zÀ-ž]+,* [A-Za-zÀ-ž]+, .+)$")

        self.parenthesis_re = re.compile(r"\s*\([^)]*\)")

        self.unclosed_re = re.compile(r"\s*\([^)]*\.")

    def parse(self) -> Generator[TranscriptSegment]:
        current_speaker = None
        current_text = []
        for p in self.soup.find_all("p"):
            b_tag = p.find("b")

            if b_tag:
                speaker_extract = self.speaker_re.match(b_tag.get_text(strip=True))

                if not speaker_extract:
                    logger.warning(f"Bold without speaker: {b_tag.get_text()}")

                yield TranscriptSegment(
                    speaker=str(current_speaker),
                    transcript=" ".join(current_text).strip(),
                )
                current_speaker = b_tag.get_text(strip=True)
                current_text = []

            else:
                if current_speaker:
                    p_text = p.get_text(separator=" ", strip=True)

                    if self.speaker_re.match(p_text):
                        logger.warning(f"Speaker found in transcription: {p_text}")

                    if p_text:
                        p_text = self.parenthesis_re.sub("", p_text)
                        p_text = self.unclosed_re.sub("", p_text)
                        current_text.append(p_text)

        if current_speaker:
            yield TranscriptSegment(
                speaker=current_speaker, transcript=" ".join(current_text).strip()
            )
