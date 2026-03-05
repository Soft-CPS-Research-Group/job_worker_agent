from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

import requests

try:  # docker is optional at import time for tooling
    from docker.types import DeviceRequest
except ImportError:  # pragma: no cover - older docker SDK or missing package
    DeviceRequest = None  # type: ignore

from .executors.base import BaseExecutor
from .executors.deucalion_executor import DeucalionExecutor
from .executors.docker_executor import DockerExecutor


_LOGGER = logging.getLogger(__name__)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class WorkerAgent:
    """Agent that polls the OPEVA backend for work and executes jobs."""

    def __init__(
        self,
        server_url: str,
        worker_id: str,
        shared_dir: str,
        image: str,
        poll_interval: float = 5.0,
        heartbeat_interval: float = 30.0,
        status_poll_interval: float = 10.0,
        exit_after_job: bool = False,
        session: Optional[requests.Session] = None,
        docker_client_factory: Optional[Callable[[], Any]] = None,
        executor: str | None = None,
        env: Mapping[str, str] | None = None,
        deucalion_executor_factory: Optional[Callable[["WorkerAgent"], BaseExecutor]] = None,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.worker_id = worker_id
        self.shared_dir = shared_dir
        self.image = image
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.status_poll_interval = status_poll_interval
        self._exit_after_job = exit_after_job
        self._last_heartbeat = 0.0
        self._stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._external_session = session is not None
        self._session = session or requests.Session()
        self._active_job_id: Optional[str] = None
        self._gpu_request_enabled = _env_flag("WORKER_ENABLE_GPU", DeviceRequest is not None)
        self._last_request_failure: Optional[str] = None
        self._env = dict(env or os.environ)

        self.executor = (executor or self._env.get("WORKER_EXECUTOR", "docker")).strip().lower()
        if self.executor == "docker":
            self._executor: BaseExecutor = DockerExecutor(self, docker_client_factory=docker_client_factory)
        elif self.executor == "deucalion":
            if deucalion_executor_factory is not None:
                self._executor = deucalion_executor_factory(self)
            else:
                self._executor = DeucalionExecutor(self, env=self._env)
        else:
            raise ValueError(f"Unknown executor '{self.executor}'. Allowed: docker, deucalion")

    # ------------------------------------------------------------------
    # Lifecycle helpers
    def _mark_active_job(self, job_id: str | None) -> None:
        self._active_job_id = job_id

    def stop(self) -> None:
        self._stop_event.set()

    def run_forever(self) -> None:
        _LOGGER.info(
            "Starting worker '%s' with executor '%s' polling %s",
            self.worker_id,
            self.executor,
            self.server_url,
        )
        self._start_heartbeat_loop()
        try:
            while not self._stop_event.is_set():
                handled = self.poll_once()
                sleep_for = 0 if handled else self.poll_interval
                if sleep_for > 0:
                    time.sleep(sleep_for)
        except KeyboardInterrupt:
            _LOGGER.info("Worker interrupted, shutting down")
        finally:
            self._stop_event.set()
            if self._heartbeat_thread:
                self._heartbeat_thread.join(timeout=2)
            self._executor.close()

    def poll_once(self) -> bool:
        self._send_heartbeat()
        job = self._request_next_job()
        if not job:
            return False
        _LOGGER.info("Received job %s", job["job_id"])
        self._run_job(job)
        if self._exit_after_job:
            _LOGGER.info("Exit-after-job flag set; stopping worker once current job completes")
            self.stop()
        return True

    def _run_job(self, job: Dict[str, Any]) -> None:
        self._executor.run_job(job)

    # ------------------------------------------------------------------
    # HTTP interactions
    def _send_heartbeat(self, force: bool = False) -> None:
        now = time.time()
        if not force:
            if self.heartbeat_interval > 0 and (now - self._last_heartbeat) < self.heartbeat_interval:
                return
        payload = {"worker_id": self.worker_id}
        _LOGGER.info("POST /api/agent/heartbeat payload=%s", payload)
        try:
            self._session.post(
                f"{self.server_url}/api/agent/heartbeat",
                json=payload,
                timeout=10,
            )
            self._last_heartbeat = now
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover - network issues
            self._handle_request_exception("heartbeat", exc, warning=True)

    def _request_next_job(self) -> Optional[Dict[str, Any]]:
        _LOGGER.info("POST /api/agent/next-job payload=%s", {"worker_id": self.worker_id})
        try:
            response = self._session.post(
                f"{self.server_url}/api/agent/next-job",
                json={"worker_id": self.worker_id},
                timeout=30,
            )
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover
            self._handle_request_exception("next-job", exc, warning=True)
            return None

        if response.status_code == 204:
            _LOGGER.info("No job available (204)")
            return None
        response.raise_for_status()
        return response.json()

    def _post_status(self, job_id: str, status: str, **extra: object) -> None:
        payload = {"job_id": job_id, "status": status, "worker_id": self.worker_id}
        payload.update({k: v for k, v in extra.items() if v is not None})
        _LOGGER.info("POST /api/agent/job-status payload=%s", payload)
        try:
            self._session.post(
                f"{self.server_url}/api/agent/job-status",
                json=payload,
                timeout=10,
            )
            self._last_request_failure = None
        except requests.RequestException as exc:  # pragma: no cover
            self._handle_request_exception(f"job-status({job_id})", exc, warning=True)

    def _fetch_status(self, job_id: str) -> Optional[str]:
        _LOGGER.info("GET /status/%s", job_id)
        try:
            response = self._session.get(f"{self.server_url}/status/{job_id}", timeout=10)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload = response.json()
            return payload.get("status")
        except requests.RequestException as exc:  # pragma: no cover
            _LOGGER.warning("Failed to fetch status for %s: %s", job_id, exc)
            return None

    # ------------------------------------------------------------------
    # Shared helpers
    def _build_command(self, job_id: str, config_path: str) -> str:
        return f"--config /data/{config_path} --job_id {job_id}"

    def _build_device_requests(self, job: Optional[Dict[str, Any]] = None) -> Optional[list]:
        job = job or {}
        if not self._gpu_request_enabled and not job.get("device_requests"):
            return None
        try:
            if job.get("device_requests"):
                return job["device_requests"]
            return [DeviceRequest(count=-1, capabilities=[["gpu"]])]
        except Exception:  # pragma: no cover - defensive
            return None

    def _build_volumes(self, vols: Optional[Any]) -> Dict[str, Dict[str, str]]:
        if isinstance(vols, list):
            out: Dict[str, Dict[str, str]] = {}
            for v in vols:
                try:
                    host = v.get("host")
                    container = v.get("container")
                    mode = v.get("mode", "rw")
                    if host and container:
                        out[host] = {"bind": container, "mode": mode}
                except Exception:
                    continue
            if out:
                return out
        return {self.shared_dir: {"bind": "/data", "mode": "rw"}}

    def _reset_session(self) -> None:
        if self._external_session:
            return
        try:
            self._session.close()
        except Exception:
            pass
        self._session = requests.Session()

    def _handle_request_exception(self, context: str, exc: requests.RequestException, warning: bool = False) -> None:
        is_repeat = self._last_request_failure == context
        log_func = _LOGGER.warning if warning and not is_repeat else (_LOGGER.debug if is_repeat else _LOGGER.error)
        log_func("Request to %s failed: %s", context, exc)
        self._reset_session()
        _LOGGER.debug("HTTP session reset after %s failure", context)
        self._last_request_failure = context

    def _build_container_name(self, job_id: str, job_name: str) -> str:
        safe_job = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in job_name)[:40]
        return f"job_{self.worker_id}_{safe_job}_{job_id[:8]}"

    def _prepare_log_file(self, job_id: str) -> Path:
        logs_dir = Path(self.shared_dir) / "jobs" / job_id / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir / f"{job_id}.log"

    def request_exit_after_current_job(self) -> None:
        """Ensure the worker stops after the currently running job."""
        self._exit_after_job = True
        if self._active_job_id is None:
            _LOGGER.info("Exit-after-job requested while idle; stopping worker immediately")
            self.stop()

    def _start_heartbeat_loop(self) -> None:
        if self.heartbeat_interval <= 0:
            return

        def _loop() -> None:
            while not self._stop_event.wait(self.heartbeat_interval):
                self._send_heartbeat()

        t = threading.Thread(target=_loop, name="heartbeat-loop", daemon=True)
        t.start()
        self._heartbeat_thread = t


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a job worker agent")
    parser.add_argument("--server", default=os.environ.get("OPEVA_SERVER", "http://localhost:8000"))
    parser.add_argument("--worker-id", default=os.environ.get("WORKER_ID"))
    parser.add_argument("--shared-dir", default=os.environ.get("OPEVA_SHARED_DIR", "/opt/opeva_shared_data"))
    parser.add_argument("--image", default=os.environ.get("WORKER_IMAGE", "calof/opeva_simulator:latest"))
    parser.add_argument("--executor", choices=("docker", "deucalion"), default=os.environ.get("WORKER_EXECUTOR", "docker"))
    parser.add_argument("--poll-interval", type=float, default=float(os.environ.get("POLL_INTERVAL", "5")))
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=float(os.environ.get("WORKER_HEARTBEAT_INTERVAL", "30")),
    )
    parser.add_argument(
        "--status-poll-interval",
        type=float,
        default=float(os.environ.get("STATUS_POLL_INTERVAL", "10")),
    )
    parser.add_argument(
        "--exit-after-job",
        action="store_true",
        default=_env_flag("WORKER_EXIT_AFTER_JOB", False),
        help="Stop the worker after completing the next job",
    )
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
