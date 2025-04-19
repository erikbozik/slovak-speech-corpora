from pydantic import BaseModel, Field, computed_field
from pydantic_settings import BaseSettings

FILENAME = "/mnt/bigben/nrsr_recordings"


class DataMetaData(BaseModel):
    data_path: str
    audio_dir_path: str
    source_part: str


class DatabaseSettings(BaseSettings):
    username: str = Field(...)
    password: str = Field(...)
    host: str = Field(...)
    port: int = Field(...)
    database: str = Field(..., validation_alias="db_name")

    class Config:
        env_file = ".env"
        env_prefix = "DB_"
        case_sensitive = False


class RecordingToProcess(BaseModel):
    id: int
    filename: str
    transcript: dict | None = None

    @computed_field
    @property
    def file_path(self) -> str:
        return f"{FILENAME}/{self.filename}"
