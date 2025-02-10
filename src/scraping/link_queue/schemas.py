from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl

from src.database import NRSRRecording


class MetaData(BaseModel):
    name: str
    category: str | None = Field(default=None)
    snapshot: datetime | None = Field(default=None)


class URLRecord(BaseModel):
    url: HttpUrl
    metadata: MetaData


class NRSRMeetingRecord(BaseModel):
    meeting_name: str
    snapshot: datetime | None
    scraped_file: bytes
    scraped_file_type: str


class NRSRRecordingData(BaseModel):
    audio: bytes
    metadata: NRSRRecording

    class Config:
        arbitrary_types_allowed = True


# class FileRecord(BaseModel):
#     name: str
#     content: bytes
