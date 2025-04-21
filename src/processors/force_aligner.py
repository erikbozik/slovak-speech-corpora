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
from pydantic import BaseModel, Field, TypeAdapter, ValidationError, computed_field
from rapidfuzz.distance import Levenshtein
from tqdm import tqdm
from whisperx import load_audio

logger = structlog.get_logger()


class GTSegment(BaseModel):
    speaker: str
    transcript: str = Field(exclude=True)

    @computed_field
    def text(self) -> str:
        return self.transcript


class WordSegment(BaseModel):
    word: str
    start: float
    end: float
    score: float
    anchored: bool | None = None
    anchor_index: int | None = None


class SpeakerSegment(BaseModel):
    speaker: str
    transcript: str
    start: float
    end: float


class VADSegment(BaseModel):
    start: float
    end: float


class ForceAligner:
    metadata: Any

    gt_adapter: TypeAdapter[list[GTSegment]]
    wt_adapter: TypeAdapter[list[WordSegment]]
    vad_adapter: TypeAdapter[list[VADSegment]]
    only_dots_and_spaces: Pattern = re.compile(r"[. ]*")

    def __init__(self) -> None:
        self.gt_adapter = TypeAdapter(list[GTSegment])
        self.wt_adapter = TypeAdapter(list[WordSegment])
        self.vad_adapter = TypeAdapter(list[VADSegment])

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

    def force_align_entire(
        self, audio: np.ndarray, segments: list[dict], vad: list[dict]
    ):
        diarize_model = whisperx.DiarizationPipeline(device="cuda")
        start = vad[0]["start"]
        sample_rate = 16000
        chunk_duration = 600
        extend_count = 0
        audio_len_sec = len(audio) / sample_rate
        final_segs: list[SpeakerSegment] = []
        while start < audio_len_sec:
            logger.info(start / audio_len_sec)
            end = min(audio_len_sec, start + chunk_duration + 300 * extend_count)
            chunk = audio[int(start * sample_rate) : int(end * sample_rate)]
            if len(chunk) == 0:
                break  # nothing to process

            df = diarize_model(chunk)
            df = df.loc[df["end"] - df["start"] > 0.7]
            df["speaker_change"] = df["speaker"] != df["speaker"].shift(1)
            df["group_id"] = df["speaker_change"].cumsum()
            last_speaker = df.iloc[-1]["speaker"]
            collapsed = (
                df.groupby("group_id")
                .filter(lambda g: g["speaker"].iloc[0] != last_speaker)
                .groupby("group_id")
                .agg({"speaker": "first", "start": "min", "end": "max"})
                .reset_index(drop=True)
            )
            segs: list[SpeakerSegment] = [
                SpeakerSegment(
                    start=row["start"] + start,
                    end=row["end"] + start,
                    transcript=seg["transcript"],
                    speaker=seg["speaker"],
                ).model_dump()
                for seg, row in zip(segments, collapsed.to_dict(orient="records"))
            ]
            if not segs:
                extend_count += 1
                logger.warning(f"Retrying to extend segment: {extend_count}")
                continue
            extend_count = 0
            segments = segments[len(collapsed) - 1 :]
            # pprint(TypeAdapter(list[SpeakerSegment]).dump_python(segs))
            start = end
            final_segs.extend(segs)
            # start = segs[-1].end

        return final_segs
        gt = self.gt_adapter.validate_python(segments)
        model_a, metadata = whisperx.load_align_model(language_code="sk", device="cuda")
        result = []
        for i in gt:
            segment_words = i.transcript.split()
            segment = i.model_dump() | {"start": start, "end": start + 300}

            segment_result = whisperx.align(
                [segment],  # type: ignore
                model_a,
                metadata,
                audio,
                "cuda",
                return_char_alignments=False,
            )
            print("first segment")
            # print(segment_result["word_segments"])
            result.append(segment_result["word_segments"])
            # start = segment_result["word_segments"][-1]["end"]
            end = segment_result["word_segments"][-1]["end"]
            end_word = segment_result["word_segments"][-1]["word"]
            while True:
                # while end_word != segment_words[-1]:
                print(end)
                segment = i.model_dump() | {"start": start, "end": end}
                segment_result = whisperx.align(
                    [segment],  # type: ignore
                    model_a,
                    metadata,
                    audio,
                    "cuda",
                    return_char_alignments=False,
                )
                end = segment_result["word_segments"][-1]["end"]
                # print(segment_result["word_segments"])
                # result.append(segment_result["word_segments"])
                # start = segment_result["word_segments"][-1]["end"]
                # end_word = segment_result["word_segments"][-1]["word"]
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
    def plus_n(iterable: list, index: int, n: int = 4):
        return range(
            min(len(iterable), index + 1),
            min(len(iterable), index + n + 1),
        )

    @staticmethod
    def minus_n(index: int, n: int = 4):
        return range(max(0, index - n), index)

    def tokenize_wt(self, wt: list[dict]) -> list[WordSegment]:
        """Return *text* split into words **and** punctuation tokens."""
        result = []

        for seg in [word for i in wt for word in i["words"]]:
            try:
                model = WordSegment.model_validate(seg)
                model.word = model.word.strip(string.punctuation).strip()
                if self.only_dots_and_spaces.fullmatch(model.word):
                    continue
                if model.word == "Ahojte":
                    # logger.warning("Ahojte present in transcript")
                    continue
                if model.word == "Ďakujem":
                    # logger.warning("Ďakujem present in transcript")
                    continue
                result.append(model)
            except ValidationError:
                continue
        return result

    def align_another_round(self, partial: list[dict], word_whisper: list[dict]):
        pass

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
        n = 4
        for idx, val in enumerate(gt_tokens):
            gt_dict[val.lower()].append(idx)
        gt_keys = list(gt_dict.keys())

        result: list[WordSegment] = []
        last_index = -1

        for i in tqdm(range(len(wt_tokens)), total=len(wt_tokens)):
            wt_word = wt_tokens[i].word.lower()
            if len(wt_word) <= 2:
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
                gt_forw = {
                    gt_tokens[j].lower() for j in self.plus_n(gt_tokens, ci, n=n)
                }
                gt_back = {gt_tokens[j].lower() for j in self.minus_n(ci, n=n)}
                wt_forw = {
                    wt_tokens[j].word.lower() for j in self.plus_n(wt_tokens, i, n=n)
                }
                wt_back = {wt_tokens[j].word.lower() for j in self.minus_n(i, n=n)}

                threshold = 2

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
                        "duration": word.start - seg_start_time,
                    }
                )
                if word.start - seg_start_time > 50:
                    pass
                    logger.warning(f"Segment too long: {word.start - seg_start_time}ms")

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
            if seg_end_time - seg_start_time > 50:
                pass
                logger.warning(f"Segment too long: {word.start - seg_start_time}ms")

        return segments
