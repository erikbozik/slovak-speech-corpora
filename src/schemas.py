from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


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
