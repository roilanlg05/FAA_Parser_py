from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class TbfmBase(DeclarativeBase):
    pass


tbfm_engine: AsyncEngine = create_async_engine(settings.tbfm_database_url, future=True, pool_pre_ping=True)
TbfmAsyncSessionLocal = async_sessionmaker(tbfm_engine, expire_on_commit=False, class_=AsyncSession)


async def get_tbfm_db_session() -> AsyncSession:
    async with TbfmAsyncSessionLocal() as session:
        yield session
