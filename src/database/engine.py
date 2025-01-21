from sqlalchemy import URL, create_engine

from ..schemas import DatabaseSettings

engine = create_engine(
    URL.create(drivername="postgresql+psycopg2", **DatabaseSettings().model_dump())  # type: ignore
)
