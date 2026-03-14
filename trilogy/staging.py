import atexit
import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class StagingType(Enum):
    LOCAL = "local"
    GCS = "gcs"
    S3 = "s3"


@dataclass
class StagingConfig:
    """Configuration for the staging directory used for temp/intermediate files.

    Supports local filesystem paths, GCS URIs (gs://), and S3 URIs (s3://).
    Defaults to the system temp directory when no path is set.
    """

    path: str | None = None

    @property
    def staging_type(self) -> StagingType:
        if self.path is None:
            return StagingType.LOCAL
        if self.path.startswith("gs://"):
            return StagingType.GCS
        if self.path.startswith("s3://"):
            return StagingType.S3
        return StagingType.LOCAL

    @property
    def resolved_root(self) -> str:
        """Return the resolved staging root path/URI, normalized with trailing separator."""
        if self.path is None:
            self.path = str(Path(tempfile.gettempdir()).resolve())
        p = self.path.rstrip("/")
        if self.staging_type == StagingType.LOCAL:
            return str(Path(p).resolve()).replace("\\", "/") + "/"
        return p + "/"

    def get_file_path(self, filename: str) -> str:
        """Return the full path/URI for a file in the staging directory."""
        return self.resolved_root + filename

    def get_executor_subdir(self, instance_id: str) -> str:
        """Return a staging subdirectory namespaced by instance_id."""
        base = self.resolved_root + instance_id + "/"
        os.makedirs(base, exist_ok=True)
        return base

    def register_cleanup(self, path: str) -> None:
        """Register atexit cleanup for a local staging path (file or directory).

        Remote paths (GCS, S3) are skipped — use bucket lifecycle policies for cleanup.
        """
        if self.staging_type != StagingType.LOCAL:
            return
        local_path = path.rstrip("/")
        os.makedirs(local_path, exist_ok=True)

        def _cleanup() -> None:
            try:
                if os.path.isdir(local_path):
                    shutil.rmtree(local_path, ignore_errors=True)
                else:
                    os.unlink(local_path)
            except OSError:
                pass

        atexit.register(_cleanup)
