from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


engine: AsyncEngine = create_async_engine(
    settings.database_url,
    future=True,
    pool_pre_ping=True,
    pool_size=settings.sfdps_db_pool_size,
    max_overflow=settings.sfdps_db_max_overflow,
    pool_timeout=settings.sfdps_db_pool_timeout,
    pool_recycle=settings.sfdps_db_pool_recycle,
    pool_use_lifo=True,
)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
