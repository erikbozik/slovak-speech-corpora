from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl


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


# class FileRecord(BaseModel):
#     name: str
#     content: bytes
