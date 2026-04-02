from __future__ import annotations

import argparse
from collections import deque
from importlib.metadata import PackageNotFoundError, version
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

_IMMEDIATE_POST_RETRIES = 3
_RETRY_INITIAL_BACKOFF_SECONDS = 0.5
_RETRY_MAX_BACKOFF_SECONDS = 5.0
_TERMINAL_QUEUE_MAX_BACKOFF_SECONDS = 30.0
_TERMINAL_JOB_STATUSES = {"finished", "failed", "stopped", "canceled"}
_JOB_THREAD_SHUTDOWN_TIMEOUT_SECONDS = 5.0


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
        self._state_lock = threading.Lock()
        self._session_lock = threading.Lock()
        self._active_jobs: dict[str, dict[str, Any]] = {}
        self._job_threads: dict[str, threading.Thread] = {}
        self._last_job_id: Optional[str] = None
        self._last_terminal_status: Optional[str] = None
        self._gpu_request_enabled = _env_flag("WORKER_ENABLE_GPU", False)
        self._last_request_failure: Optional[str] = None
        self._has_processed_job = False
        self._pending_terminal_statuses: deque[dict[str, Any]] = deque()
        self._pending_terminal_statuses_lock = threading.Lock()
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
        self.max_active_jobs = self._resolve_max_active_jobs()
        self._worker_version = self._resolve_worker_version()

    # ------------------------------------------------------------------
    # Lifecycle helpers
    def _mark_active_job(self, job_id: str | None) -> None:
        # Backward-compatible helper used by older tests/executors.
        if job_id is None:
            with self._state_lock:
                if len(self._active_jobs) == 1:
                    only_job_id = next(iter(self._active_jobs.keys()))
                    self._active_jobs.pop(only_job_id, None)
            return
        self._register_active_job(job_id)

    def _resolve_max_active_jobs(self) -> int:
        default_slots = 3 if self.executor == "deucalion" else 1
        raw_value = self._env.get("WORKER_MAX_ACTIVE_JOBS")
        if raw_value is None and self.executor == "deucalion":
            raw_value = self._env.get("DEUCALION_MAX_ACTIVE_JOBS")
        if raw_value is None:
            return default_slots
        try:
            parsed = int(str(raw_value).strip())
        except (TypeError, ValueError):
            _LOGGER.warning("Invalid WORKER_MAX_ACTIVE_JOBS=%r; using %d", raw_value, default_slots)
            return default_slots
        if parsed < 1:
            _LOGGER.warning("WORKER_MAX_ACTIVE_JOBS must be >= 1; using %d", default_slots)
            return default_slots
        return parsed

    def _register_active_job(self, job_id: str, job_name: str | None = None) -> None:
        now = time.time()
        with self._state_lock:
            self._active_jobs[job_id] = {
                "job_id": job_id,
                "job_name": (job_name or "").strip() or None,
                "status": "dispatched",
                "phase": "accepted",
                "started_at": now,
                "updated_at": now,
            }

    def _unregister_active_job(self, job_id: str) -> None:
        with self._state_lock:
            self._active_jobs.pop(job_id, None)

    def _update_active_job(self, job_id: str, **fields: object) -> None:
        with self._state_lock:
            entry = self._active_jobs.get(job_id)
            if entry is None:
                return
            for key, value in fields.items():
                if value is None:
                    entry.pop(key, None)
                else:
                    entry[key] = value
            entry["updated_at"] = time.time()

    def _active_jobs_snapshot(self) -> list[dict[str, Any]]:
        with self._state_lock:
            rows = [dict(value) for value in self._active_jobs.values()]
        rows.sort(key=lambda row: float(row.get("started_at") or 0.0))
        return rows

    def _active_job_count(self) -> int:
        with self._state_lock:
            return len(self._active_jobs)

    def _running_thread_count(self) -> int:
        with self._state_lock:
            return len(self._job_threads)

    def _available_slots(self) -> int:
        return max(0, self.max_active_jobs - self._running_thread_count())

    def _resolve_worker_version(self) -> str:
        if self._env.get("WORKER_VERSION"):
            return str(self._env["WORKER_VERSION"])
        try:
            return version("job-worker-agent")
        except PackageNotFoundError:
            return "dev"

    def _build_heartbeat_info(self) -> Dict[str, Any]:
        active_jobs = self._active_jobs_snapshot()
        active_job_ids = [str(entry.get("job_id")) for entry in active_jobs if entry.get("job_id")]
        first_active = active_jobs[0] if active_jobs else None
        with self._state_lock:
            last_job_id = self._last_job_id
            last_terminal_status = self._last_terminal_status
        info: Dict[str, Any] = {
            "executor": self.executor,
            "worker_version": self._worker_version,
            "gpu_enabled": self._gpu_request_enabled,
            "max_active_jobs": self.max_active_jobs,
            "active_job_id": first_active.get("job_id") if first_active else None,
            "active_job_count": len(active_jobs),
            "active_job_ids": active_job_ids,
            "active_jobs": active_jobs,
            "last_job_id": last_job_id,
            "last_terminal_status": last_terminal_status,
        }
        if first_active and first_active.get("status"):
            info["active_job_status"] = first_active.get("status")
        try:
            executor_info = self._executor.heartbeat_info()
            if isinstance(executor_info, dict):
                info.update(executor_info)
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("Failed to collect executor heartbeat info: %s", exc)
        return info

    def stop(self) -> None:
        self._stop_event.set()

    def run_forever(self) -> None:
        _LOGGER.info(
            "Starting worker '%s' with executor '%s' polling %s (max_active_jobs=%d)",
            self.worker_id,
            self.executor,
            self.server_url,
            self.max_active_jobs,
        )
        try:
            self._executor.on_startup()
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("Executor startup hook failed: %s", exc)
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
            self._join_job_threads(timeout=_JOB_THREAD_SHUTDOWN_TIMEOUT_SECONDS)
            self._executor.close()

    def poll_once(self) -> bool:
        self._reap_finished_job_threads()
        self._flush_pending_terminal_statuses()
        self._send_heartbeat()
        self._flush_pending_terminal_statuses()
        handled = False

        while not self._stop_event.is_set() and self._available_slots() > 0:
            job = self._request_next_job()
            if not job:
                break
            _LOGGER.info("Received job %s", job["job_id"])
            self._has_processed_job = True
            handled = True
            if self.max_active_jobs <= 1:
                self._run_job(job)
                break
            self._start_job_thread(job)
            if self._exit_after_job:
                break

        self._flush_pending_terminal_statuses(force=True)
        if self._exit_after_job and self._has_processed_job and self._running_thread_count() == 0:
            _LOGGER.info("Exit-after-job flag set; stopping worker once current job completes")
            self.stop()
        return handled

    def _run_job(self, job: Dict[str, Any]) -> None:
        self._executor.run_job(job)

    def _start_job_thread(self, job: Dict[str, Any]) -> None:
        job_id = str(job.get("job_id", "unknown"))

        def _runner() -> None:
            try:
                self._run_job(job)
            except Exception:
                _LOGGER.exception("Unhandled worker exception while executing job %s", job_id)
            finally:
                with self._state_lock:
                    self._job_threads.pop(job_id, None)
                self._flush_pending_terminal_statuses(force=True)

        thread = threading.Thread(target=_runner, name=f"job-{job_id[:8]}", daemon=True)
        with self._state_lock:
            self._job_threads[job_id] = thread
        thread.start()

    def _reap_finished_job_threads(self) -> None:
        with self._state_lock:
            finished = [job_id for job_id, thread in self._job_threads.items() if not thread.is_alive()]
            for job_id in finished:
                self._job_threads.pop(job_id, None)

    def _join_job_threads(self, timeout: float) -> None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._state_lock:
                threads = list(self._job_threads.values())
            if not threads:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            join_for = min(0.2, remaining)
            for thread in threads:
                thread.join(timeout=join_for)
            self._reap_finished_job_threads()

        remaining_jobs = self._running_thread_count()
        if remaining_jobs > 0:
            _LOGGER.warning(
                "Worker shutdown reached timeout with %d active job thread(s) still running",
                remaining_jobs,
            )

    # ------------------------------------------------------------------
    # HTTP interactions
    def _is_retryable_http_status(self, status_code: int) -> bool:
        return status_code in {408, 429} or 500 <= status_code < 600

    def _is_retryable_exception(self, exc: requests.RequestException) -> bool:
        if isinstance(exc, requests.HTTPError):
            response = exc.response
            if response is None:
                return False
            return self._is_retryable_http_status(response.status_code)
        return True

    def _post_json_with_retries(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        *,
        context: str,
        timeout: float = 10,
        warning: bool = False,
    ) -> Dict[str, bool]:
        backoff = _RETRY_INITIAL_BACKOFF_SECONDS
        last_retryable_exc: Optional[requests.RequestException] = None
        for attempt in range(1, _IMMEDIATE_POST_RETRIES + 1):
            try:
                with self._session_lock:
                    response = self._session.post(
                        f"{self.server_url}{endpoint}",
                        json=payload,
                        timeout=timeout,
                    )
                response.raise_for_status()
                self._last_request_failure = None
                return {"ok": True, "retryable": False}
            except requests.RequestException as exc:
                retryable = self._is_retryable_exception(exc)
                if not retryable:
                    is_repeat = self._last_request_failure == context
                    log_func = _LOGGER.warning if warning and not is_repeat else (
                        _LOGGER.debug if is_repeat else _LOGGER.error
                    )
                    status_code = getattr(getattr(exc, "response", None), "status_code", "n/a")
                    log_func(
                        "Request to %s failed with non-retryable response (status=%s): %s",
                        context,
                        status_code,
                        exc,
                    )
                    self._last_request_failure = context
                    return {"ok": False, "retryable": False}

                last_retryable_exc = exc
                self._handle_request_exception(context, exc, warning=warning)
                if attempt >= _IMMEDIATE_POST_RETRIES:
                    break
                sleep_for = min(backoff, _RETRY_MAX_BACKOFF_SECONDS)
                _LOGGER.warning(
                    "Retrying %s in %.1fs (attempt %d/%d)",
                    context,
                    sleep_for,
                    attempt + 1,
                    _IMMEDIATE_POST_RETRIES,
                )
                time.sleep(sleep_for)
                backoff = min(backoff * 2, _RETRY_MAX_BACKOFF_SECONDS)
        if last_retryable_exc is not None:
            _LOGGER.warning("Exhausted retries for %s: %s", context, last_retryable_exc)
        return {"ok": False, "retryable": True}

    def _enqueue_pending_terminal_status(self, payload: Dict[str, Any]) -> None:
        job_id = payload.get("job_id")
        status = payload.get("status")
        with self._pending_terminal_statuses_lock:
            for pending in self._pending_terminal_statuses:
                existing = pending.get("payload", {})
                if existing.get("job_id") == job_id and existing.get("status") == status:
                    return
            self._pending_terminal_statuses.append(
                {
                    "payload": dict(payload),
                    "next_retry_at": time.monotonic(),
                    "backoff_seconds": _RETRY_INITIAL_BACKOFF_SECONDS,
                }
            )
        _LOGGER.warning(
            "Queued terminal status for retry (job=%s status=%s)",
            job_id,
            status,
        )

    def _flush_pending_terminal_statuses(self, force: bool = False) -> None:
        while True:
            with self._pending_terminal_statuses_lock:
                if not self._pending_terminal_statuses:
                    return
                entry = self._pending_terminal_statuses[0]
                if not force and entry["next_retry_at"] > time.monotonic():
                    return
                entry = self._pending_terminal_statuses.popleft()

            payload = entry["payload"]
            job_id = payload.get("job_id", "unknown")
            result = self._post_json_with_retries(
                "/api/agent/job-status",
                payload,
                context=f"job-status({job_id})",
                timeout=10,
                warning=True,
            )
            if result["ok"]:
                continue

            if result["retryable"]:
                backoff = min(
                    max(float(entry.get("backoff_seconds", _RETRY_INITIAL_BACKOFF_SECONDS)) * 2, _RETRY_INITIAL_BACKOFF_SECONDS),
                    _TERMINAL_QUEUE_MAX_BACKOFF_SECONDS,
                )
                entry["backoff_seconds"] = backoff
                entry["next_retry_at"] = time.monotonic() + backoff
                with self._pending_terminal_statuses_lock:
                    self._pending_terminal_statuses.appendleft(entry)
                _LOGGER.warning(
                    "Will retry pending terminal status for job %s in %.1fs",
                    job_id,
                    backoff,
                )
                return

            _LOGGER.error("Dropping pending terminal status for job %s after non-retryable response", job_id)

    def _send_heartbeat(self, force: bool = False) -> None:
        now = time.time()
        if not force:
            if self.heartbeat_interval > 0 and (now - self._last_heartbeat) < self.heartbeat_interval:
                return
        payload = {
            "worker_id": self.worker_id,
            "info": self._build_heartbeat_info(),
        }
        _LOGGER.info("POST /api/agent/heartbeat payload=%s", payload)
        result = self._post_json_with_retries(
            "/api/agent/heartbeat",
            payload,
            context="heartbeat",
            timeout=10,
            warning=True,
        )
        if result["ok"]:
            self._last_heartbeat = now

    def _request_next_job(self) -> Optional[Dict[str, Any]]:
        _LOGGER.info("POST /api/agent/next-job payload=%s", {"worker_id": self.worker_id})
        try:
            with self._session_lock:
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

    def _update_active_job_from_status(self, job_id: str, status: str, extra: dict[str, object]) -> None:
        details = extra.get("details")
        detail_map = details if isinstance(details, dict) else {}
        phase = detail_map.get("executor_stage")
        if not isinstance(phase, str) or not phase:
            phase = status
        slurm_job_id = detail_map.get("slurm_job_id") if isinstance(detail_map.get("slurm_job_id"), str) else None
        slurm_state = detail_map.get("slurm_state") if isinstance(detail_map.get("slurm_state"), str) else None
        slurm_partition = (
            detail_map.get("slurm_partition") if isinstance(detail_map.get("slurm_partition"), str) else None
        )
        slurm_nodes = detail_map.get("slurm_nodes")
        slurm_cpus = detail_map.get("slurm_cpus")
        slurm_gpus = detail_map.get("slurm_gpus")
        queue_pos = detail_map.get("slurm_queue_position")
        ahead = detail_map.get("slurm_jobs_ahead")
        updated_fields: dict[str, object] = {
            "status": status,
            "phase": phase,
            "slurm_job_id": slurm_job_id,
            "slurm_state": slurm_state,
            "slurm_partition": slurm_partition,
            "slurm_nodes": slurm_nodes,
            "slurm_cpus": slurm_cpus,
            "slurm_gpus": slurm_gpus,
            "queue_pos": queue_pos,
            "ahead": ahead,
        }
        self._update_active_job(job_id, **updated_fields)

    def _post_status(self, job_id: str, status: str, **extra: object) -> None:
        with self._state_lock:
            self._last_job_id = job_id
            if status in _TERMINAL_JOB_STATUSES:
                self._last_terminal_status = status
        self._update_active_job_from_status(job_id, status, dict(extra))
        payload = {"job_id": job_id, "status": status, "worker_id": self.worker_id}
        payload.update({k: v for k, v in extra.items() if v is not None})
        _LOGGER.info("POST /api/agent/job-status payload=%s", payload)
        result = self._post_json_with_retries(
            "/api/agent/job-status",
            payload,
            context=f"job-status({job_id})",
            timeout=10,
            warning=True,
        )
        if not result["ok"] and result["retryable"] and status in _TERMINAL_JOB_STATUSES:
            self._enqueue_pending_terminal_status(payload)

    def _fetch_status(self, job_id: str) -> Optional[str]:
        _LOGGER.info("GET /status/%s", job_id)
        try:
            with self._session_lock:
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
        with self._session_lock:
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
        if self._running_thread_count() == 0:
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
