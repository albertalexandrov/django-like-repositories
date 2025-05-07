from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from config import settings

engine = create_async_engine(settings.db.dsn, echo=True)
session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)
metadata = MetaData()
