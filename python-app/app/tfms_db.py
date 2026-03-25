from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class TfmsBase(DeclarativeBase):
    pass


tfms_engine: AsyncEngine = create_async_engine(settings.tfms_database_url, future=True, pool_pre_ping=True)
TfmsAsyncSessionLocal = async_sessionmaker(tfms_engine, expire_on_commit=False, class_=AsyncSession)


async def get_tfms_db_session() -> AsyncSession:
    async with TfmsAsyncSessionLocal() as session:
        yield session
