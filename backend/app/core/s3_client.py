"""
core/s3_client.py
-----------------
Singleton boto3 S3 client for migration staging.
Wraps upload, download and presigned URL operations.

Usage:
    from app.core.s3_client import get_s3_client

    s3 = get_s3_client()
    await s3.upload_file(local_path, s3_key)
"""

import asyncio
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_s3_client = None


def init_s3_client() -> None:
    """Initialize boto3 S3 client. Called once during lifespan startup."""
    global _s3_client
    settings = get_settings().s3

    _s3_client = boto3.client(
        "s3",
        region_name=settings.region,
        aws_access_key_id=settings.access_key_id.get_secret_value(),
        aws_secret_access_key=settings.secret_access_key.get_secret_value(),
    )
    logger.info("S3 client initialized", bucket=settings.bucket, region=settings.region)


def get_s3_client() -> "S3Client":
    if _s3_client is None:
        raise RuntimeError("S3 client not initialized.")
    return S3Client(_s3_client)


class S3Client:
    """Async-friendly wrapper around boto3 S3 client."""

    def __init__(self, client) -> None:
        self._client = client
        self._settings = get_settings().s3

    @property
    def bucket(self) -> str:
        return self._settings.bucket

    @property
    def prefix(self) -> str:
        return self._settings.prefix

    def _full_key(self, key: str) -> str:
        return f"{self.prefix.rstrip('/')}/{key.lstrip('/')}"

    async def upload_file(self, local_path: str | Path, s3_key: str) -> str:
        """Upload a local file to S3. Returns the full S3 key."""
        full_key = self._full_key(s3_key)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.upload_file(str(local_path), self.bucket, full_key),
        )
        logger.info("File uploaded to S3", s3_key=full_key, local_path=str(local_path))
        return full_key

    async def download_file(self, s3_key: str, local_path: str | Path) -> None:
        full_key = self._full_key(s3_key)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.download_file(self.bucket, full_key, str(local_path)),
        )

    async def list_objects(self, prefix: str = "") -> list[dict]:
        """List objects under a prefix. Returns list of {key, size, last_modified}."""
        full_prefix = self._full_key(prefix) if prefix else self.prefix
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._client.list_objects_v2(Bucket=self.bucket, Prefix=full_prefix),
        )
        return [
            {
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            }
            for obj in response.get("Contents", [])
        ]

    async def delete_object(self, s3_key: str) -> None:
        full_key = self._full_key(s3_key)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._client.delete_object(Bucket=self.bucket, Key=full_key),
        )

    async def object_exists(self, s3_key: str) -> bool:
        full_key = self._full_key(s3_key)
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.head_object(Bucket=self.bucket, Key=full_key),
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    async def test_connection(self) -> dict:
        """Health check."""
        import time
        try:
            start = time.monotonic()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.head_bucket(Bucket=self.bucket),
            )
            elapsed_ms = round((time.monotonic() - start) * 1000, 2)
            return {"status": "ok", "bucket": self.bucket, "latency_ms": elapsed_ms}
        except Exception as exc:
            return {"status": "error", "error": str(exc)}
