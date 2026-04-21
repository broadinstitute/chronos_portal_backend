from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import json
import os

from .routes import upload, qc, reports, chronos_run, differential_dependency, preprocessing
from .services.connection_manager import manager
from .services.job_manager import job_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    job_manager.ensure_directories()
    yield


app = FastAPI(title="Chronos Analysis Portal", lifespan=lifespan)

cors_origins = os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router, prefix="/api")
app.include_router(preprocessing.router, prefix="/api")
app.include_router(qc.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(chronos_run.router, prefix="/api")
app.include_router(differential_dependency.router, prefix="/api")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            if message.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
            elif message.get("type") == "ack" and message.get("msg_id"):
                manager.acknowledge(message["msg_id"])
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/api/health")
async def health_check():
    return {"status": "ok"}
