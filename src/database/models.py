from sqlalchemy import JSON, Column, Float, Integer, LargeBinary, String
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
    speaker_id = Column(Integer)
    speaker_gender = Column(String, nullable=False)
    sampling_rate = Column(Integer)
    other_data = Column(JSON)
