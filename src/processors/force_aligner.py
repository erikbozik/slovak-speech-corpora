import gc
import re
import string
from collections import defaultdict
from re import Pattern
from typing import Any

import numpy as np
import structlog
import torch
import whisperx
from pydantic import BaseModel, TypeAdapter, ValidationError
from rapidfuzz.distance import Levenshtein
from tqdm import tqdm
from whisperx import load_audio

logger = structlog.get_logger()


class GTSegment(BaseModel):
    speaker: str
    transcript: str


class WordSegment(BaseModel):
    word: str
    start: float
    end: float
    score: float
    anchored: bool | None = None
    anchor_index: int | None = None


class ForceAligner:
    metadata: Any

    gt_adapter: TypeAdapter[list[GTSegment]]
    wt_adapter: TypeAdapter[list[WordSegment]]
    only_dots_and_spaces: Pattern = re.compile(r"[. ]*")

    def __init__(self) -> None:
        self.gt_adapter = TypeAdapter(list[GTSegment])
        self.wt_adapter = TypeAdapter(list[WordSegment])

    def load_audio(self, file_path: str):
        logger.debug("Loading audio", file_path=file_path)
        audio = load_audio(file_path)
        logger.debug("Audio loaded", file_path=file_path)
        return audio

    def transcribe(self, audio: np.ndarray):
        logger.debug("Transcribing audio")
        model = whisperx.load_model("large-v3", device="cuda", language="sk")
        result = model.transcribe(audio, language="sk")
        logger.debug("Transcription complete")
        gc.collect()
        torch.cuda.empty_cache()
        del model
        return result

    def force_align(self, audio: np.ndarray, segments: dict):
        model_a = None
        try:
            model_a, metadata = whisperx.load_align_model(
                language_code="sk", device="cuda"
            )
            result = whisperx.align(
                segments,
                model_a,
                metadata,
                audio,
                "cuda",
                return_char_alignments=False,
            )
        except torch.cuda.OutOfMemoryError:
            logger.error("CUDA OOM error")
            model_a, metadata = whisperx.load_align_model(
                language_code="sk", device="cpu"
            )
            result = whisperx.align(
                segments,
                model_a,
                metadata,
                audio,
                "cpu",
                return_char_alignments=False,
            )
        gc.collect()
        torch.cuda.empty_cache()
        del model_a
        return result

    @staticmethod
    def plus_n(iterable: list, index: int):
        return range(
            min(len(iterable), index + 1),
            min(len(iterable), index + 5),
        )

    @staticmethod
    def minus_n(index: int):
        return range(max(0, index - 4), index)

    def tokenize_wt(self, wt: list[dict]) -> list[WordSegment]:
        """Return *text* split into words **and** punctuation tokens."""
        result = []

        for seg in [word for i in wt for word in i["words"]]:
            try:
                model = WordSegment.model_validate(seg)
                model.word = model.word.strip(string.punctuation).strip()
                if self.only_dots_and_spaces.match(model.word):
                    continue
                result.append(model)
            except ValidationError:
                continue
        return result

    def align(
        self, gt_db: list[dict], wt_tokens_db: list[dict], max_jump: int = 50
    ) -> list[WordSegment]:
        """
        Align wt_tokens to gt_tokens by:
        1. Fuzzy‐matching word forms (distance ≤ 1) to build candidates
        2. Validating with a forward/back context intersection score
        """
        gt = self.gt_adapter.validate_python(gt_db)
        wt_tokens = self.tokenize_wt(wt_tokens_db)
        # 1) build lookup of all GT positions per lower‐cased word
        gt_tokens = " ".join(item.transcript for item in gt)
        gt_tokens = list(map(lambda x: x.strip(string.punctuation), gt_tokens.split()))
        gt_dict = defaultdict(list)
        for idx, val in enumerate(gt_tokens):
            gt_dict[val.lower()].append(idx)
        gt_keys = list(gt_dict.keys())

        result: list[WordSegment] = []
        last_index = -1

        for i in tqdm(range(len(wt_tokens)), total=len(wt_tokens)):
            wt_word = wt_tokens[i].word.lower()
            if len(wt_word) <= 3:
                continue

            # 2) collect all GT indices whose key is within distance ≤1 of wt_word
            candidates = []
            for key in gt_keys:
                # length‐based shortcut: if lengths differ by >1, skip
                if abs(len(key) - len(wt_word)) > 1:
                    continue
                # exact or one‐edit away
                if key == wt_word or Levenshtein.distance(key, wt_word) <= 1:
                    for ci in gt_dict[key]:
                        if ci > last_index and (ci - last_index) <= max_jump:
                            candidates.append(ci)

            # try each candidate in ascending order
            for ci in sorted(candidates):
                # build your forward/back context sets
                gt_forw = {gt_tokens[j].lower() for j in self.plus_n(gt_tokens, ci)}
                gt_back = {gt_tokens[j].lower() for j in self.minus_n(ci)}
                wt_forw = {wt_tokens[j].word.lower() for j in self.plus_n(wt_tokens, i)}
                wt_back = {wt_tokens[j].word.lower() for j in self.minus_n(i)}

                # adjust threshold if context‐windows differ
                threshold = 1
                if (
                    abs(len(gt_forw) - len(wt_forw)) > 1
                    or abs(len(gt_back) - len(wt_back)) > 1
                ):
                    threshold = 2

                # fuzzy context score (exact intersection here; swap in levenshtein if desired)
                score = len(gt_forw & wt_forw) + len(gt_back & wt_back)

                if score > threshold:
                    wt_tokens[i].anchored = True
                    wt_tokens[i].anchor_index = ci
                    # remove so we don’t re‐use the same GT slot
                    # note: we use the lowercase key to pop from gt_dict
                    matched_key = gt_tokens[ci].lower()
                    gt_dict[matched_key].remove(ci)
                    result.append(wt_tokens[i])
                    last_index = ci
                    break  # move on to next wt_token

        return result

    def segment(
        self,
        aligned: list[WordSegment],
        gt_words: list[str],
        max_duration: float = 15.0,
    ):
        """
        Use a (possibly sparse) list of aligned WordSegment objects to stamp
        the whole transcript.  Each JSON segment:

        * starts with the first word that fits,
        * ends right *before* the word that would exceed `max_duration`,
        * contains every transcript token between those two anchors, so
            nothing is left behind.
        """
        # ------------------------------------------------------------------ #
        # 1. read the ground‑truth tokens into a flat list
        # ------------------------------------------------------------------ #

        if not aligned:
            print("[]")
            return []

        segments = []
        seg_start_idx = aligned[0].anchor_index  # first token in segment
        seg_start_time = aligned[0].start
        seg_end_time = aligned[0].end  # will grow while inside
        # the same segment

        # ------------------------------------------------------------------ #
        # 2. walk through the *rest* of the aligned words
        # ------------------------------------------------------------------ #
        iterating = False
        for word in aligned[1:]:
            iterating = True
            prospective_end = word.end

            # 2a. Would this word send the chunk over the limit?
            if prospective_end - seg_start_time > max_duration:
                # ---- close the current segment WITHOUT <word> -------------
                segments.append(
                    {
                        "start": seg_start_time,
                        "end": word.start,  # keeps timeline tight
                        "text": " ".join(
                            gt_words[seg_start_idx : word.anchor_index]  # ← key line
                        ),
                    }
                )
                # ---- open a new one that STARTS WITH <word> --------------
                seg_start_idx = word.anchor_index
                seg_start_time = word.start
                seg_end_time = word.end
            else:
                # 2b. still within the same chunk – keep growing it
                seg_end_time = prospective_end

        # ------------------------------------------------------------------ #
        # 3. flush the tail (everything after the last cut)
        # ------------------------------------------------------------------ #
        if iterating:
            segments.append(
                {
                    "start": seg_start_time,
                    "end": seg_end_time,
                    "text": " ".join(gt_words[seg_start_idx : word.anchor_index + 1]),
                    "duration": word.start - seg_start_time,
                }
            )

        return segments
