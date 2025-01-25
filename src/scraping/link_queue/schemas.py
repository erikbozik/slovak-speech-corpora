from pydantic import BaseModel, HttpUrl


class MetaData(BaseModel):
    name: str


class URLRecord(BaseModel):
    url: HttpUrl
    metadata: MetaData
