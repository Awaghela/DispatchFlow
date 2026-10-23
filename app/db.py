from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

settings = get_settings()


class Base(DeclarativeBase):
    pass


engine = create_async_engine(
    settings.database_url,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,  # transparently replaces connections killed by a DB restart / failover
    pool_recycle=1800,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

_SCHEMA_LOCK_ID = 727_274  # arbitrary; serialises concurrent create_all from api + worker


async def init_db() -> None:
    from . import models  # noqa: F401  (register tables)

    async with engine.connect() as conn:
        await conn.execute(text("SELECT pg_advisory_lock(:id)"), {"id": _SCHEMA_LOCK_ID})
        try:
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()
        finally:
            await conn.execute(text("SELECT pg_advisory_unlock(:id)"), {"id": _SCHEMA_LOCK_ID})
            await conn.commit()
