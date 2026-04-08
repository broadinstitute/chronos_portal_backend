# Chronos Portal Server

FastAPI backend for the CRISPR Analysis Portal.

## Requirements

- Python 3.12+
- chronos package (`pip install crispr_chronos`)
- FastAPI and dependencies (`pip install fastapi uvicorn[standard] python-multipart pypdf`)

## Running

```bash
uvicorn app.main:app --reload
```

Server runs on http://localhost:8000

## Structure

```
server/
├── app/
│   ├── main.py                  # FastAPI app, WebSocket endpoint, CORS config
│   ├── routes/
│   │   ├── upload.py            # File upload, library selection
│   │   ├── qc.py                # Initial QC analysis
│   │   ├── chronos_run.py       # Chronos training, post-QC, hit calling, file downloads
│   │   ├── differential_dependency.py  # Condition comparison analysis
│   │   └── reports.py           # Job listing, report retrieval, PDF/image serving
│   └── services/
│       ├── job_manager.py       # Singleton: job state, file tracking, config persistence
│       ├── connection_manager.py # WebSocket broadcast to all clients
│       ├── data_loader.py       # Load/validate readcounts, condition_map, guide_map
│       ├── file_utils.py        # CSV/TSV/HDF5 parsing utilities
│       ├── concurrency.py       # matplotlib_lock for thread-safe plotting
│       └── logging_utils.py     # send_log() for real-time job progress
├── Jobs/                        # Per-job directories
│   └── {job_id}/
│       ├── config.json          # Job metadata and file info
│       ├── title.txt            # Job display name
│       ├── uploads/             # Uploaded data files
│       ├── Reports/             # Generated PNGs and PDFs
│       ├── ChronosOutput/       # HDF5 model outputs
│       └── CSVOutputs/          # CSV exports (gene_effect, FDR, etc.)
└── Logs/                        # {job_id}.log files
```

## API Endpoints

### Upload & Setup
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/libraries` | List available built-in sgRNA libraries |
| POST | `/api/set-library/{library_name}` | Use a built-in library (avana, ky) |
| POST | `/api/upload/{file_type}` | Upload a file (readcounts, condition_map, guide_map, copy_number, positive_controls, negative_controls) |
| POST | `/api/new-job` | Create a new job |
| GET | `/api/job-status` | Get current job status |

### Analysis
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/run-qc` | Run initial QC analysis |
| POST | `/api/run-chronos` | Run Chronos training + post-QC + hit calling |
| POST | `/api/run-differential-dependency` | Compare two conditions |

### Reports & Downloads
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/jobs` | List all completed jobs |
| GET | `/api/jobs/{job_id}/log` | Get job log file content |
| GET | `/api/jobs/{job_id}/conditions` | Get available conditions for comparison |
| GET | `/api/reports/{job_id}` | Get initial QC report metadata |
| GET | `/api/reports/{job_id}/chronos-qc` | Get post-Chronos QC report sections |
| GET | `/api/reports/{job_id}/hits` | Get hit calling report sections |
| GET | `/api/reports/{job_id}/differential-dependency` | Get differential dependency report sections |
| GET | `/api/reports/{job_id}/image/{filename}` | Serve a report image |
| GET | `/api/reports/{job_id}/pdf` | Download initial QC PDF |
| GET | `/api/reports/{job_id}/hits/pdf` | Download hits PDF |
| GET | `/api/reports/{job_id}/differential-dependency/pdf` | Download differential dependency PDF |
| GET | `/api/outputs/{job_id}` | List all output files |
| GET | `/api/outputs/{job_id}/download/{filename}` | Download a single file |
| POST | `/api/outputs/{job_id}/download-zip` | Download multiple files as ZIP |

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| WS | `/ws` | WebSocket for real-time updates |

## WebSocket Messages

Server broadcasts to all connected clients:

```json
{"type": "status", "status": "running|complete|chronos_complete|qc_report_ready|hits_report_ready|dd_report_ready", "message": "...", "job_id": "..."}
{"type": "error", "error": "...", "job_id": "..."}
{"type": "log", "log": "full log content", "job_id": "..."}
```

Client can send:
```json
{"type": "ping"}  // Server responds with {"type": "pong"}
```

## Analysis Pipeline

1. **Upload** - Files saved to `Jobs/{job_id}/uploads/`, metadata stored in `config.json`
2. **Initial QC** - `chronos.reports.qc_initial_data()` generates PNGs + PDF in `Reports/`
3. **Chronos Training** - `chronos.Chronos().train()` saves model to `ChronosOutput/`
4. **Post-Chronos QC** - `chronos.reports.dataset_qc_report()` generates QC report
5. **Hit Calling** - `chronos.hit_calling` computes p-values, FDR; generates hits report
6. **Differential Dependency** (optional) - `ConditionComparison` compares two conditions
