import asyncio
import json
import uuid
from datetime import datetime

from fastapi import WebSocket, WebSocketDisconnect

from app.core.logging import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    def __init__(self) -> None:
        # job_id → set of active WebSocket connections
        self._connections: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.setdefault(job_id, set()).add(websocket)
        logger.info("WebSocket connected", job_id=job_id)

    async def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            connections = self._connections.get(job_id, set())
            connections.discard(websocket)
            if not connections:
                self._connections.pop(job_id, None)
        logger.info("WebSocket disconnected", job_id=job_id)

    async def broadcast(self, job_id: str, message: dict) -> None:
        async with self._lock:
            connections = set(self._connections.get(job_id, set()))

        if not connections:
            return

        dead = set()
        payload = json.dumps(message, default=str)

        for ws in connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)

        # Clean up dead connections
        if dead:
            async with self._lock:
                active = self._connections.get(job_id, set())
                active -= dead

    async def send_progress(self, job_id: str, progress_data: dict, log_line: str | None = None) -> None:
        await self.broadcast(job_id, {
            "type": "progress",
            "job_id": job_id,
            "data": progress_data,
            "log": log_line,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def send_done(self, job_id: str, summary: dict) -> None:
        await self.broadcast(job_id, {
            "type": "done",
            "job_id": job_id,
            "data": summary,
            "timestamp": datetime.utcnow().isoformat(),
        })

    async def send_error(self, job_id: str, error: str) -> None:
        await self.broadcast(job_id, {
            "type": "error",
            "job_id": job_id,
            "error": error,
            "timestamp": datetime.utcnow().isoformat(),
        })

    def active_job_ids(self) -> list[str]:
        return list(self._connections.keys())

    def connection_count(self, job_id: str | None = None) -> int:
        if job_id:
            return len(self._connections.get(job_id, set()))
        return sum(len(v) for v in self._connections.values())

# Global singleton — shared across all requests
ws_manager = ConnectionManager()

async def handle_progress_websocket(websocket: WebSocket, job_id: str) -> None:
    # Validate UUID
    try:
        uuid.UUID(job_id)
    except ValueError:
        await websocket.close(code=1008, reason="Invalid job_id format")
        return

    await ws_manager.connect(job_id, websocket)

    try:
        # Send current state immediately so client doesn't wait for next update
        from app.core.database import _session_factory
        from app.services.migration.state import get_job_state_service

        if _session_factory:
            async with _session_factory() as db:
                svc = get_job_state_service(db)
                progress = await svc.get_progress(uuid.UUID(job_id))
                if progress:
                    await websocket.send_text(json.dumps({
                        "type": "progress",
                        "job_id": job_id,
                        "data": progress.model_dump(mode="json"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }, default=str))

        # Heartbeat loop — keeps connection alive and detects disconnects
        while True:
            try:
                # Wait for client message (ping) or timeout
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if data == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                # Send server-side ping
                try:
                    await websocket.send_text(json.dumps({"type": "ping"}))
                except Exception:
                    break
            except WebSocketDisconnect:
                break

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("WebSocket error", job_id=job_id, error=str(exc))
    finally:
        await ws_manager.disconnect(job_id, websocket)
