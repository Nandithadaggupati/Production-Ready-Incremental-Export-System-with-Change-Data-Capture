import asyncpg
from app.config import settings
from app.logging_config import logger

_pool = None

async def init_db_pool():
    global _pool
    if _pool is None:
        try:
            logger.info("Initializing PostgreSQL database connection pool...")
            _pool = await asyncpg.create_pool(
                dsn=settings.DATABASE_URL,
                min_size=2,
                max_size=10
            )
            logger.info("Database pool initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise e

async def get_db_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        await init_db_pool()
    return _pool

async def close_db_pool():
    global _pool
    if _pool is not None:
        logger.info("Closing PostgreSQL database connection pool...")
        await _pool.close()
        _pool = None
        logger.info("Database pool closed.")
