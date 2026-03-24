import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from unittest.mock import AsyncMock, MagicMock

from app.core.database import Base

# Test DB 

TEST_DB_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://maf_user:maf_dev_password@localhost:5432/maf_test"
)

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()

@pytest_asyncio.fixture
async def db_session(test_engine) -> AsyncSession:
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()

# FastAPI test client

@pytest_asyncio.fixture
async def client(db_session):
    from app.main import app
    from app.core.database import get_db_session

    async def override_db():
        yield db_session

    app.dependency_overrides[get_db_session] = override_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()

# Mock fixtures

@pytest.fixture
def mock_oracle_pool():
    pool = MagicMock()
    cursor = AsyncMock()
    cursor.description = [("ID",), ("DT_REFERENCIA",), ("VALUE",)]
    cursor.fetchall.return_value = [(1, "2024-01-01", 100.0)]
    cursor.fetchone.return_value = (999_999,)  # Count query
    conn = AsyncMock()
    conn.cursor.return_value.__aenter__ = AsyncMock(return_value=cursor)
    conn.cursor.return_value.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool

@pytest.fixture
def mock_snowflake_engine():
    engine = MagicMock()
    conn = AsyncMock()
    result = MagicMock()
    result.fetchone.return_value = (999_999,)
    result.fetchall.return_value = [(1, "2024-01-01", 100.0)]
    result.keys.return_value = ["ID", "DT_REFERENCIA", "VALUE"]
    conn.execute.return_value = result
    engine.connect.return_value.__aenter__ = AsyncMock(return_value=conn)
    engine.connect.return_value.__aexit__ = AsyncMock(return_value=False)
    return engine
