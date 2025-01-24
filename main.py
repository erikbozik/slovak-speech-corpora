import structlog
from sqlalchemy.orm import sessionmaker

from metadata import COMMON_VOICE_METADATA, FLEURS_METADATA, VOXPOPULI_METADATA
from src.database import Base, engine
from src.extractors import CommonVoice, Fleurs, VoxPopuli

logger = structlog.get_logger(level="INFO")
Base.metadata.create_all(bind=engine)
s_maker = sessionmaker(bind=engine)


def insert(extractor):
    with s_maker() as session:
        for count, recording in enumerate(extractor.extract()):
            session.add(recording)
            if count % 50:
                session.commit()
        session.commit()


def extract_data():
    logger.info("Processing Fleurs")
    for i in FLEURS_METADATA:
        a = Fleurs(i)
        logger.info(f"Processing {i.source_part}")
        insert(a)

    logger.info("Processing Common Voice")
    for i in COMMON_VOICE_METADATA:
        a = CommonVoice(i)
        logger.info(f"Processing {i.source_part}")
        insert(a)

    logger.info("Processing Voxpopuli")
    for i in VOXPOPULI_METADATA:
        a = VoxPopuli(i)
        logger.info(f"Processing {i.source_part}")
        insert(a)


extract_data()
