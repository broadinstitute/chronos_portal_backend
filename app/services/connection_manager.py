from fastapi import WebSocket
import asyncio


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connections (no retry)."""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

    async def broadcast_with_retry(self, message: dict, retry_duration: float = 30.0):
        """Broadcast message with retry for specified duration (default 30s)."""
        end_time = asyncio.get_event_loop().time() + retry_duration
        while asyncio.get_event_loop().time() < end_time:
            await self.broadcast(message)
            await asyncio.sleep(5)

    async def send_status(self, status: str, message: str, job_id: str = None, data: dict = None):
        payload = {
            "type": "status",
            "status": status,
            "message": message,
        }
        if job_id:
            payload["job_id"] = job_id
        if data:
            payload["data"] = data
        asyncio.create_task(self.broadcast_with_retry(payload))

    async def send_error(self, error: str, job_id: str = None):
        asyncio.create_task(self.broadcast_with_retry({
            "type": "error",
            "error": error,
            "job_id": job_id,
        }))


manager = ConnectionManager()
