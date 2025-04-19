import re
from typing import Any

import emoji
import jiwer


class WerProcessor:
    def __init__(self):
        pass

    def load_whisper_transcript(self, wt: dict[str, Any]):
        return "".join([i["text"] for i in wt["segments"]])

    def load_real_transcript(self, rt: list[dict[str, Any]]):
        return " ".join([i["transcript"] for i in rt])

    def clean_whisper_text(self, whisper_t: str):
        whisper_t = whisper_t.replace("...", "")
        whisper_t = emoji.replace_emoji(whisper_t, replace="")
        whisper_t = whisper_t.lower()
        whisper_t = re.sub(r"\s+", " ", whisper_t).strip()
        whisper_t = re.sub(r"[^\w\s]", "", whisper_t)
        return whisper_t

    def clean_real_text(self, real_t: str):
        real_t = real_t.lower()
        real_t = re.sub(r"[^\w\s]", "", real_t)
        real_t = re.sub(r"\s+", " ", real_t).strip()
        return real_t

    def wer(self, whisper_t: str, real_t: str):
        return jiwer.wer(whisper_t, real_t)

    def process(self, wt: dict[str, Any], rt: list[dict[str, Any]]):
        whisper_t = self.load_whisper_transcript(wt)
        real_t = self.load_real_transcript(rt)
        whisper_t = self.clean_whisper_text(whisper_t)
        real_t = self.clean_real_text(real_t)
        wer = self.wer(whisper_t, real_t)
        return wer
