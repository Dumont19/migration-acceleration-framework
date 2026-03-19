"""
core/config.py
--------------
Centralized configuration using Pydantic Settings v2.
Reads from environment variables or .env file.
All other modules import `get_settings()` — never read os.environ directly.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class OracleSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ORACLE_", env_file=".env", extra="ignore")

    host: str = Field(..., description="Oracle DB hostname or IP")
    port: int = Field(1521, description="Oracle listener port")
    service: str = Field(..., description="Oracle service name")
    user: str = Field(..., description="Oracle username")
    password: SecretStr = Field(..., description="Oracle password")
    schema_name: str = Field("DWADM", alias="ORACLE_SCHEMA", description="Source schema")
    pool_min: int = Field(2, description="Minimum connection pool size")
    pool_max: int = Field(10, description="Maximum connection pool size")
    pool_increment: int = Field(1, description="Pool increment step")


class SnowflakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SNOWFLAKE_", env_file=".env", extra="ignore")

    account: str = Field(..., description="Snowflake account identifier")
    user: str = Field(..., description="Snowflake username (SSO email)")
    role: str = Field("SYSADMIN", description="Snowflake role")
    warehouse: str = Field("WH_COMPUTE", description="Snowflake warehouse")
    database: str = Field("DWDEV", description="Target database")
    schema_name: str = Field("DWADM", alias="SNOWFLAKE_SCHEMA", description="Target schema")
    # Optional: password-based auth (if not using SSO)
    password: SecretStr | None = Field(None, description="Password (leave empty for SSO)")
    authenticator: str = Field("externalbrowser", description="Auth method: externalbrowser | snowflake | oauth")


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AWS_", env_file=".env", extra="ignore")

    access_key_id: SecretStr = Field(..., alias="AWS_ACCESS_KEY_ID")
    secret_access_key: SecretStr = Field(..., alias="AWS_SECRET_ACCESS_KEY")
    region: str = Field("us-east-1", alias="AWS_REGION")
    bucket: str = Field(..., alias="S3_BUCKET", description="Migration staging bucket")
    prefix: str = Field("migration/", alias="S3_PREFIX", description="Key prefix for uploaded files")


class DatabaseSettings(BaseSettings):
    """PostgreSQL — persistent log storage."""
    model_config = SettingsConfigDict(env_prefix="DB_", env_file=".env", extra="ignore")

    host: str = Field("localhost", alias="DB_HOST")
    port: int = Field(5432, alias="DB_PORT")
    name: str = Field("maf_logs", alias="DB_NAME")
    user: str = Field("maf_user", alias="DB_USER")
    password: SecretStr = Field(..., alias="DB_PASSWORD")
    pool_size: int = Field(5, alias="DB_POOL_SIZE")
    max_overflow: int = Field(10, alias="DB_MAX_OVERFLOW")
    echo_sql: bool = Field(False, alias="DB_ECHO_SQL", description="Log SQL statements (dev only)")

    @property
    def url(self) -> str:
        pwd = self.password.get_secret_value()
        return f"postgresql+asyncpg://{self.user}:{pwd}@{self.host}:{self.port}/{self.name}"

    @property
    def sync_url(self) -> str:
        """For Alembic migrations (sync driver)."""
        pwd = self.password.get_secret_value()
        return f"postgresql+psycopg2://{self.user}:{pwd}@{self.host}:{self.port}/{self.name}"


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = Field("Migration Acceleration Framework", alias="APP_NAME")
    app_version: str = Field("4.0.0", alias="APP_VERSION")
    environment: Literal["development", "staging", "production"] = Field(
        "development", alias="APP_ENV"
    )
    debug: bool = Field(False, alias="APP_DEBUG")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        "INFO", alias="LOG_LEVEL"
    )
    cors_origins: list[str] = Field(
        default=["http://localhost:3000"],
        alias="CORS_ORIGINS",
        description="Allowed CORS origins (Next.js dev server)",
    )
    ws_heartbeat_interval: int = Field(30, description="WebSocket ping interval in seconds")
    max_parallel_workers: int = Field(4, alias="MAX_WORKERS", description="Parallel migration workers")

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v


class Settings:
    """
    Aggregated settings container.
    Access via: settings = get_settings()
    """

    def __init__(self) -> None:
        self.app = AppSettings()
        self.oracle = OracleSettings()
        self.snowflake = SnowflakeSettings()
        self.s3 = S3Settings()
        self.db = DatabaseSettings()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Returns a cached Settings instance.
    Use FastAPI's Depends(get_settings) for injection,
    or call directly in non-request contexts.
    """
    return Settings()
