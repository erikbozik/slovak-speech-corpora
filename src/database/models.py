from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    LargeBinary,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Recording(Base):
    __tablename__ = "recording"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False, unique=True)
    transcript = Column(String)
    audio = Column(LargeBinary, nullable=False)
    source = Column(String, nullable=False)
    source_part = Column(String)
    duration_ms = Column(Float, nullable=False)
    audio_size = Column(Float, nullable=False)
    speaker_id = Column(String)
    speaker_gender = Column(String)
    sampling_rate = Column(Integer)
    other_data = Column(JSONB)


class NRSRTranscript(Base):
    __tablename__ = "nrsr_transcripts"

    id = Column(Integer, primary_key=True)
    meeting_name = Column(String)
    meeting_num = Column(Integer)
    snapshot = Column(Date)
    scraped_file = Column(LargeBinary, nullable=False)
    scraped_file_type = Column(String, nullable=False)
    xhtml_parsed = Column(String)
    json_parsed = Column(JSONB)


class NRSRRecording(Base):
    __tablename__ = "nrsr_recording"

    id = Column(Integer, primary_key=True)
    meeting_name = Column(String)
    meeting_num = Column(Integer)
    snapshot = Column(Date)
    audio_format = Column(String)
    audio_size = Column(Float)
    duration = Column(Float)
    sampling_rate = Column(Integer)


class Members(Base):
    __tablename__ = "members"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    surname = Column(String)
    term = Column(Integer)
