# Chronos Portal Server

FastAPI backend for the CRISPR Analysis Portal.

## Requirements

- Python 3.12+
- chronos package (`pip install crispr_chronos`)
- FastAPI and dependencies (`pip install fastapi uvicorn[standard] python-multipart`)

## Running

```bash
uvicorn app.main:app --reload
```

Server runs on http://localhost:8000

## Structure

```
server/
├── app/
│   ├── main.py              # FastAPI app, WebSocket endpoint, CORS config
│   ├── routes/
│   │   ├── upload.py        # File upload endpoints
│   │   └── qc.py            # QC analysis endpoint
│   └── services/
│       ├── connection_manager.py  # WebSocket broadcast manager
│       └── job_manager.py         # Job creation, file tracking, state recovery
├── Jobs/                    # Job directories (uploads, reports)
└── Logs/                    # Job log files
```

## API Endpoints

- `POST /api/upload/{file_type}` - Upload a file (readcounts, condition_map, guide_map, etc.)
- `POST /api/run-qc` - Start QC analysis
- `POST /api/new-job` - Create a new job
- `GET /api/job-status` - Get current job status
- `GET /api/health` - Health check
- `WS /ws` - WebSocket for real-time status updates

## WebSocket Messages

Server sends:
- `{"type": "status", "status": "running|complete", "message": "...", "job_id": "..."}`
- `{"type": "error", "error": "...", "job_id": "..."}`
