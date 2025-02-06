from pydantic import HttpUrl

from src.scraping.link_queue import MetaData, URLRecord

dl_links = [
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=9"),
        metadata=MetaData(name="nrsr2023"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=8"),
        metadata=MetaData(name="nrsr2020"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=7"),
        metadata=MetaData(name="nrsr2016"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=6"),
        metadata=MetaData(name="nrsr2012"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=5"),
        metadata=MetaData(name="nrsr2010"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=4"),
        metadata=MetaData(name="nrsr2006"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=3"),
        metadata=MetaData(name="nrsr2002"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=2"),
        metadata=MetaData(name="nrsr1998"),
    ),
    URLRecord(
        url=HttpUrl("https://www.nrsr.sk/dl/Browser/Default?legId=13&termNr=1"),
        metadata=MetaData(name="nrsr1994"),
    ),
]

recording_links = [
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/9"),
        metadata=MetaData(name="nrsr2023"),
    ),
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/8"),
        metadata=MetaData(name="nrsr2020"),
    ),
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/7"),
        metadata=MetaData(name="nrsr2016"),
    ),
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/6"),
        metadata=MetaData(name="nrsr2012"),
    ),
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/5"),
        metadata=MetaData(name="nrsr2010"),
    ),
    URLRecord(
        url=HttpUrl("https://tv.nrsr.sk/archiv/schodza/4"),
        metadata=MetaData(name="nrsr2006"),
    ),
]
