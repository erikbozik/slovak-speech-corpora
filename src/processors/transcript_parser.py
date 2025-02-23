import re
from re import Pattern

import structlog
from bs4 import BeautifulSoup
from pydantic import BaseModel
from redis import Redis

from src.redis_client import redis_factory

logger = structlog.get_logger()


class TranscriptSegment(BaseModel):
    speaker: str
    transcript: str


class TranscriptParser:
    content: str
    redis_client: Redis | None
    soup: BeautifulSoup
    parenthesis_re: Pattern
    unclosed_re: Pattern

    def __init__(self, xhtml: str) -> None:
        self.content = xhtml
        self.soup = BeautifulSoup(self.content, "html.parser")

        self.parenthesis_re = re.compile(r"\s*\([^)]*\)")
        self.parenthesis2_re = re.compile(r"\s*\[[^)]*\]")

        self.unclosed_re = re.compile(r"\s*\([^)]*\.")
        self.unclosed2_re = re.compile(r"\s*\[[^)]*\.")
        self.client = None

    @staticmethod
    def clean_and_split(text):
        """
        Custom splitting function which splits the sentence
        on spaces, delimiters. In the final split, the delimiters
        are omitted.
        """
        words_and_delimiters = re.findall(r"\w+|[^\w\s]", text)
        return [word for word in words_and_delimiters if word.isalnum()]

    def match_speaker(self, line):
        words = self.clean_and_split(line)
        if not words:
            return

        number_of_members = sum(self.client.smismember("members", words))  # type: ignore

        # a line can have any number of names from 1 to 3
        # Andrej, Danko - 1
        # A. Danko - 2
        # Lucia Duris Nicholsonova - 3
        if 0 < number_of_members < 4:
            # 'návrh' is often found in bold
            # the word is used in the context of
            # 'návrh poslancov Andreja Danka, Lucie ...'
            if "návrh" in line:
                return

            # in some cases the role's representation is long
            # yet it is still a speaker definition
            check_length = True
            # from research these are the two cases we need to include
            if "poverený" in line and "vedením" in line:
                logger.warning(f"{line} has 'povereny vedenim'")
                check_length = False
            elif "poverená" in line and "riadením" in line:
                logger.warning(f"{line} has 'Matecna'")
                check_length = False

            # checking for the sentence length
            if check_length and len(words) - number_of_members > 15:
                logger.info(
                    f"{words} has more than {len(words) - number_of_members} words"
                )
                return
            return True
        return False

    def parse(self) -> list[dict]:
        # initialize the redis client
        # for parallel processing to work
        if self.client is None:
            self.client = redis_factory()
        result = []
        current_speaker = None
        current_text = []
        # go through all the <p> tags
        for p in self.soup.find_all("p", class_=False):
            # find bold in the <p>
            b_tag = p.find("b")

            # if <b> tag is present than check if it is a speaker
            if b_tag and self.match_speaker(b_tag.get_text(strip=True)):
                # in case of a new speaker found we must
                # write the previous one and initialize new
                if current_speaker:
                    result.append(
                        TranscriptSegment(
                            speaker=str(current_speaker),
                            transcript=" ".join(current_text).strip(),
                        )
                    )

                current_speaker = b_tag.get_text(strip=True)
                current_text = []

            else:
                # in case of no b_tag we simply add the text to the current speaker
                # speaker must be initialized
                if current_speaker:
                    p_text = p.get_text(separator=" ", strip=True)

                    if p_text:
                        # substitute all the unnecessary text
                        p_text = self.parenthesis_re.sub("", p_text)
                        p_text = self.parenthesis2_re.sub("", p_text)
                        p_text = self.unclosed_re.sub("", p_text)
                        p_text = self.unclosed2_re.sub("", p_text)
                        current_text.append(p_text)

        # write the last speaker
        if current_speaker:
            result.append(
                TranscriptSegment(
                    speaker=current_speaker, transcript=" ".join(current_text).strip()
                )
            )

        # return the last one
        return [i.model_dump() for i in result]
