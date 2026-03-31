import json
import re
from datetime import datetime
from pathlib import Path


class JobManager:
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent.parent
        self.jobs_dir = self.base_dir / "Jobs"
        self.logs_dir = self.base_dir / "Logs"
        self.current_job_id: str = None
        self.current_job_dir: Path = None
        self.current_title: str = None
        self.uploaded_files: dict = {}
        self.job_config: dict = {}

    def ensure_directories(self):
        self.jobs_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)

    def _sanitize_name(self, name: str) -> str:
        """Sanitize job name for use in directory/file names."""
        if not name:
            return "job"
        # Replace spaces and special chars with underscores
        sanitized = re.sub(r'[^\w\-]', '_', name)
        # Remove consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Trim underscores from ends
        return sanitized.strip('_')[:50] or "job"

    def create_job(self, job_name: str = None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name_prefix = self._sanitize_name(job_name) if job_name else "job"
        job_id = f"{name_prefix}_{timestamp}"
        job_dir = self.jobs_dir / job_id
        job_dir.mkdir(exist_ok=True)
        (job_dir / "Reports").mkdir(exist_ok=True)
        (job_dir / "uploads").mkdir(exist_ok=True)

        self.current_job_id = job_id
        self.current_job_dir = job_dir
        self.current_title = job_name or "Untitled Analysis"
        self.uploaded_files = {}

        # Initialize job config
        self.job_config = {
            "job_id": job_id,
            "title": self.current_title,
            "created_at": datetime.now().isoformat(),
            "files": {},
            "compare_conditions": None,
        }
        self._save_config()

        # Save title to file for recovery
        (job_dir / "title.txt").write_text(self.current_title)

        return job_id

    def resume_job(self, job_id: str):
        """Resume an existing job, recovering file paths from disk."""
        job_dir = self.jobs_dir / job_id
        if not job_dir.exists():
            # Job doesn't exist, create it
            job_dir.mkdir(exist_ok=True)
            (job_dir / "Reports").mkdir(exist_ok=True)
            (job_dir / "uploads").mkdir(exist_ok=True)

        self.current_job_id = job_id
        self.current_job_dir = job_dir

        # Recover title from file
        title_file = job_dir / "title.txt"
        if title_file.exists():
            self.current_title = title_file.read_text().strip()
        else:
            self.current_title = "Untitled Analysis"

        # Recover uploaded files from disk
        uploads_dir = job_dir / "uploads"
        if uploads_dir.exists():
            self.uploaded_files = {}
            for file_path in uploads_dir.iterdir():
                if file_path.is_file():
                    # Extract file type from filename (e.g., "readcounts.csv" -> "readcounts")
                    file_type = file_path.stem
                    self.uploaded_files[file_type] = file_path

        # Load config if exists
        self._load_config()

    def get_title(self) -> str:
        return self.current_title or "Untitled Analysis"

    def get_job_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id

    def get_reports_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id / "Reports"

    def get_uploads_dir(self, job_id: str) -> Path:
        return self.jobs_dir / job_id / "uploads"

    def get_log_path(self, job_id: str) -> Path:
        return self.logs_dir / f"{job_id}.log"

    def store_file_path(self, file_type: str, path: Path):
        self.uploaded_files[file_type] = path

    def get_file_path(self, file_type: str) -> Path:
        return self.uploaded_files.get(file_type)

    def clear_uploads(self):
        self.uploaded_files = {}

    def _get_config_path(self) -> Path:
        return self.current_job_dir / "config.json"

    def _save_config(self):
        if self.current_job_dir:
            with open(self._get_config_path(), "w") as f:
                json.dump(self.job_config, f, indent=2)

    def _load_config(self):
        config_path = self._get_config_path()
        if config_path.exists():
            with open(config_path, "r") as f:
                self.job_config = json.load(f)
        else:
            self.job_config = {
                "job_id": self.current_job_id,
                "title": self.current_title,
                "files": {},
                "compare_conditions": None,
            }

    def add_file_info(self, file_type: str, original_filename: str, file_format: str):
        """Record file metadata in job config."""
        self.job_config["files"][file_type] = {
            "original_filename": original_filename,
            "format": file_format,
            "uploaded_at": datetime.now().isoformat(),
        }
        self._save_config()

    def set_compare_conditions(self, condition1: str, condition2: str):
        """Set compare conditions in job config."""
        if condition1 or condition2:
            self.job_config["compare_conditions"] = {
                "condition1": condition1,
                "condition2": condition2,
            }
            self._save_config()

    def mark_qc_started(self):
        """Mark QC as started in job config."""
        self.job_config["qc_started_at"] = datetime.now().isoformat()
        self._save_config()

    def mark_qc_completed(self):
        """Mark QC as completed in job config."""
        self.job_config["qc_completed_at"] = datetime.now().isoformat()
        self._save_config()

    def mark_chronos_started(self):
        """Mark Chronos as started in job config."""
        self.job_config["chronos_started_at"] = datetime.now().isoformat()
        self._save_config()

    def mark_chronos_completed(self):
        """Mark Chronos as completed in job config."""
        self.job_config["chronos_completed_at"] = datetime.now().isoformat()
        self._save_config()

    def get_config(self) -> dict:
        return self.job_config

    def get_file_format(self, file_type: str) -> str:
        """Get the user-specified format for a file type."""
        file_info = self.job_config.get("files", {}).get(file_type, {})
        return file_info.get("format", "csv")


job_manager = JobManager()
