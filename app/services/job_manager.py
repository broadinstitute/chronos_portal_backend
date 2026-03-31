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


job_manager = JobManager()
