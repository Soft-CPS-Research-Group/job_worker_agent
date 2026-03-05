from __future__ import annotations

from typing import Any, Dict, Protocol


class WorkerRuntime(Protocol):
    """Runtime callbacks and properties exposed by WorkerAgent to executors."""

    worker_id: str
    image: str
    status_poll_interval: float
    shared_dir: str

    def _post_status(self, job_id: str, status: str, **extra: object) -> None:
        ...

    def _fetch_status(self, job_id: str) -> str | None:
        ...

    def _send_heartbeat(self, force: bool = False) -> None:
        ...

    def _build_command(self, job_id: str, config_path: str) -> str:
        ...

    def _build_container_name(self, job_id: str, job_name: str) -> str:
        ...

    def _build_volumes(self, vols: Any) -> Dict[str, Dict[str, str]]:
        ...

    def _build_device_requests(self, job: Dict[str, Any] | None = None) -> list | None:
        ...

    def _prepare_log_file(self, job_id: str):
        ...

    def _mark_active_job(self, job_id: str | None) -> None:
        ...


class BaseExecutor:
    def run_job(self, job: Dict[str, Any]) -> None:
        raise NotImplementedError

    def close(self) -> None:
        return None
