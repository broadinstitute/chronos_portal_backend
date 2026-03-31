from fastapi import WebSocket
import json


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
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

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
        await self.broadcast(payload)

    async def send_error(self, error: str, job_id: str = None):
        await self.broadcast({
            "type": "error",
            "error": error,
            "job_id": job_id,
        })


manager = ConnectionManager()
