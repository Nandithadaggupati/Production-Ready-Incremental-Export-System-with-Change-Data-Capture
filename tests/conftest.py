import pytest
from typing import AsyncGenerator
import asyncpg
from app.database import get_db_pool, init_db_pool, close_db_pool
from app.main import app
from httpx import AsyncClient

# Database pool fixture - function scoped to align with pytest-asyncio's event loop
@pytest.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    await init_db_pool()
    pool = await get_db_pool()
    yield pool
    await close_db_pool()

# HTTP client fixture
@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="http://test") as ac:
        yield ac
