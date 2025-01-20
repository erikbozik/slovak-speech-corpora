from sqlalchemy import Column, Float, Integer, LargeBinary, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Recording(Base):
    __tablename__ = "recording"

    id = Column(Integer, primary_key=True)
    filename = Column(String, nullable=False, unique=True)
    transcript = Column(String, nullable=False)
    audio = Column(LargeBinary, nullable=False)
    duration_sec = Column(Float, nullable=False)
    audio_size_mb = Column(Float, nullable=False)
    speaker_id = Column(Integer)
    speaker_gender = Column(String, nullable=False)
