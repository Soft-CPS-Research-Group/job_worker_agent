from __future__ import annotations

import errno
import hashlib
import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
import shutil
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Mapping

from worker_agent.union.archive import (
    append_missing_algorithm_logs,
    create_input_archive,
    merge_job_results,
    safe_extract,
    write_result_storage_manifest,
)
from worker_agent.union.client import (
    FlyteUnionClient,
    UnionRunSnapshot,
    derive_object_uris,
    deterministic_run_name,
)
from worker_agent.union.config import UnionConfig
from worker_agent.union.events import parse_event

from .base import BaseExecutor, StaleJobAttemptError, WorkerRuntime


_LOGGER = logging.getLogger(__name__)
_TERMINAL_BACKEND_STATUSES = {"finished", "failed", "stopped", "canceled"}
_STOP_BACKEND_STATUSES = {"stop_requested", "stopped", "canceled"}
_GPU_MODEL_LOG_MARKER = "CUDA device selected:"
_GPU_MODEL_LOG_SCAN_BYTES = 512 * 1024
_AUTHENTICATION_ERROR_MARKERS = (
    "unauthenticated",
    "authentication failed",
    "token expired",
    "authorization pending",
    "provide credentials",
    "invalid api key",
)


def _normalize_gpu_model(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())[:160]
    return normalized or None


def _gpu_model_from_log(path: Path) -> str | None:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            content = handle.read(_GPU_MODEL_LOG_SCAN_BYTES)
    except OSError:
        return None
    for line in content.splitlines():
        if _GPU_MODEL_LOG_MARKER in line:
            return _normalize_gpu_model(line.rsplit(_GPU_MODEL_LOG_MARKER, 1)[1])
    return None


class _RecoveryInterrupted(RuntimeError):
    pass


class _RecoveryStopped(RuntimeError):
    pass


class UnionExecutor(BaseExecutor):
    def __init__(
        self,
        runtime: WorkerRuntime,
        *,
        env: Mapping[str, str] | None = None,
        client_factory: Callable[[UnionConfig], Any] | None = None,
        now_fn: Callable[[], float] = time.time,
        wait_fn: Callable[[float], bool] | None = None,
    ) -> None:
        self.runtime = runtime
        self.env = dict(env or os.environ)
        self.config = UnionConfig.from_env(self.env)
        self.client = (client_factory or FlyteUnionClient)(self.config)
        self.now_fn = now_fn
        self._state_lock = threading.RLock()
        self._recovery_threads: dict[str, threading.Thread] = {}
        self._claimed_jobs: set[str] = set()
        self._recovery_waiting: set[str] = set()
        self._recovery_active: set[str] = set()
        self._recovery_slots = threading.BoundedSemaphore(self.config.max_concurrent_recoveries)
        self._closing = threading.Event()
        self._last_recovery_request_scan_at = 0.0
        self.wait_fn = wait_fn or self._closing.wait

    def heartbeat_info(self) -> Dict[str, Any]:
        self._scan_recovery_requests_if_due()
        info = {
            "gpu_enabled": True,
            "gpu_required": True,
            "union_endpoint": self.config.endpoint,
            "union_project": self.config.project,
            "union_domain": self.config.domain,
            "union_gpu_count": self.config.gpu_count,
            "union_auth_mode": self.config.auth_mode,
            "union_max_concurrent_recoveries": self.config.max_concurrent_recoveries,
        }
        with self._state_lock:
            info["union_recovery_waiting_count"] = len(self._recovery_waiting)
            info["union_recovery_active_count"] = len(self._recovery_active)
        if self.config.auth_mode == "device_flow":
            ensure_fresh = getattr(self.client, "ensure_authentication_fresh", None)
            if callable(ensure_fresh):
                ensure_fresh(self.config.auth_verify_interval_seconds)
            info["union_auth"] = self.client.auth_state()
        return info

    def ready_for_new_jobs(self) -> bool:
        if self.config.auth_mode != "device_flow":
            return True
        return self.client.auth_state().get("status") == "authenticated"

    def handle_command(self, command: Dict[str, Any]) -> None:
        if command.get("action") == "union_authenticate":
            self.client.start_device_authentication(str(command.get("request_id") or ""))
        elif command.get("action") == "union_recover_job":
            job_id = str(command.get("job_id") or "")
            request_id = str(command.get("request_id") or "")
            if self._start_requested_recovery(job_id):
                self._clear_recovery_request(job_id, request_id)

    def _job_dir(self, job_id: str) -> Path:
        return Path(self.runtime.shared_dir) / "jobs" / job_id

    def _state_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / ".worker" / "union.json"

    def _recovery_request_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / ".worker" / "union-recovery-request.json"

    def _load_state(self, job_id: str) -> dict[str, Any] | None:
        path = self._state_path(job_id)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _save_state(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        path = self._state_path(job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name("union.json.tmp")
        with self._state_lock:
            state["updated_at"] = self.now_fn()
            serialized = json.dumps(dict(state), indent=2, sort_keys=True)
            temporary.write_text(serialized, encoding="utf-8")
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)

    def _clear_recovery_request(self, job_id: str, request_id: str = "") -> None:
        path = self._recovery_request_path(job_id)
        if request_id:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                return
            except (OSError, json.JSONDecodeError):
                payload = {}
            persisted_id = str(payload.get("request_id") or "") if isinstance(payload, dict) else ""
            if persisted_id and persisted_id != request_id:
                return
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def _fetch_backend_status_with_presence(self, job_id: str) -> tuple[str | None, bool | None]:
        lookup = getattr(self.runtime, "_fetch_status_with_presence", None)
        if callable(lookup):
            return lookup(job_id)
        status = self.runtime._fetch_status(job_id)
        return status, True if status is not None else None

    def _retire_missing_job_state(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        state["terminal"] = True
        state["orchestrator_ack"] = True
        state["orchestrator_status"] = "deleted"
        state["orchestrator_missing_at"] = self.now_fn()
        state.setdefault("terminal_status", "canceled")
        state["terminal_stage"] = "union:orchestrator_job_missing"
        state["recovery_status"] = "discarded"
        self._save_state(state)
        self._clear_recovery_request(job_id)
        _LOGGER.info(
            "Ignoring persisted Union state for deleted orchestrator job %s",
            job_id,
        )

    def _scan_recovery_requests_if_due(self, *, force: bool = False) -> None:
        now = self.now_fn()
        interval = max(5, min(30, self.config.recovery_retry_interval_seconds))
        with self._state_lock:
            if not force and now - self._last_recovery_request_scan_at < interval:
                return
            self._last_recovery_request_scan_at = now

        jobs_root = Path(self.runtime.shared_dir) / "jobs"
        if not jobs_root.is_dir():
            return
        for request_path in jobs_root.glob("*/.worker/union-recovery-request.json"):
            try:
                request = json.loads(request_path.read_text(encoding="utf-8"))
                if not isinstance(request, dict):
                    continue
                job_id = str(request.get("job_id") or request_path.parents[1].name)
                backend_status, backend_exists = self._fetch_backend_status_with_presence(job_id)
                if backend_exists is False:
                    state = self._load_state(job_id)
                    if state:
                        self._retire_missing_job_state(state)
                    self._clear_recovery_request(job_id, str(request.get("request_id") or ""))
                    continue
                if backend_status != "recovering":
                    continue
                if self._start_requested_recovery(job_id):
                    self._clear_recovery_request(job_id, str(request.get("request_id") or ""))
            except Exception as exc:
                _LOGGER.warning("Unable to process durable Union recovery request %s: %s", request_path, exc)

    def _claim_job(self, job_id: str) -> bool:
        with self._state_lock:
            if job_id in self._claimed_jobs:
                return False
            self._claimed_jobs.add(job_id)
            return True

    def _release_job(self, job_id: str) -> None:
        with self._state_lock:
            self._claimed_jobs.discard(job_id)

    def _abort_revoked_run(self, state: dict[str, Any]) -> bool:
        run_name = str(state.get("run_name") or "").strip()
        aborted = not bool(state.get("submitted"))
        if run_name and state.get("submitted"):
            try:
                self.client.abort(run_name, reason="OPEVA execution attempt was superseded")
                aborted = True
            except Exception as exc:  # pragma: no cover - depends on Union availability
                _LOGGER.warning("Unable to abort revoked Union run %s: %s", run_name, exc)
                state["revocation_error"] = str(exc)
        state["revoked"] = True
        state["revoked_at"] = self.now_fn()
        if aborted:
            state["terminal"] = True
            state["terminal_status"] = "canceled"
            state["terminal_stage"] = "union:revoked"
            state["orchestrator_ack"] = True
            state.pop("revocation_error", None)
        self._save_state(state)
        return aborted

    def _active_status(self, state: dict[str, Any]) -> str:
        if state.get("compute_succeeded") and state.get("artifact_deleted") is not True:
            return "recovering"
        return "running" if state.get("started_at") else "setup"

    def _active_details(self, state: dict[str, Any], *, stage: str, **extra: Any) -> dict[str, Any]:
        return {
            "executor_stage": stage,
            "started_at": state.get("started_at"),
            "gpu_model": state.get("gpu_model"),
            "union_run_id": state.get("run_name"),
            "union_run_url": state.get("run_url"),
            "union_phase": state.get("union_phase"),
            "compute_succeeded": state.get("compute_succeeded") is True,
            "compute_finished_at": state.get("compute_finished_at"),
            "recovery_status": state.get("recovery_status"),
            "recovery_error": state.get("recovery_error"),
            **extra,
        }

    @staticmethod
    def _is_authentication_error(exc: Exception) -> bool:
        response = getattr(exc, "response", None)
        if getattr(response, "status_code", None) == 401:
            return True
        message = str(exc).lower()
        return any(marker in message for marker in _AUTHENTICATION_ERROR_MARKERS)

    def _handle_authentication_error(self, exc: Exception) -> None:
        if self.config.auth_mode != "device_flow":
            return
        invalidate = getattr(self.client, "invalidate_authentication", None)
        if callable(invalidate):
            invalidate(exc)
        self.client.start_device_authentication()

    def _mark_control_plane_verified(self, stage: str) -> None:
        mark_verified = getattr(self.client, "mark_verified", None)
        if callable(mark_verified):
            if "log" in stage:
                surface = "pod_logs"
            elif "artifact" in stage:
                surface = "artifact_signer"
            else:
                surface = "api"
            mark_verified(surface)

    def _is_retryable_control_plane_error(self, exc: Exception, *, stage: str = "") -> bool:
        if isinstance(exc, (ValueError, TypeError, KeyError, FileNotFoundError)):
            return False
        if isinstance(exc, OSError) and exc.errno in {
            errno.EACCES,
            errno.EDQUOT,
            errno.ENOSPC,
            errno.EROFS,
        }:
            return False
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        if isinstance(status_code, int) and 400 <= status_code < 500:
            if self.config.auth_mode == "device_flow" and status_code == 401:
                return True
            return status_code in {408, 409, 425, 429}
        message = str(exc).lower()
        if self.config.auth_mode == "device_flow" and self._is_authentication_error(exc):
            return True
        if stage in {"polling_run", "refreshing_artifact"} and "not found" in message:
            # Runs and freshly uploaded result objects can briefly be absent
            # from the corresponding read path.
            return True
        permanent_markers = (
            "permission denied",
            "unauthenticated",
            "invalid argument",
            "invalid api key",
            "authentication failed",
            "not found",
        )
        return not any(marker in message for marker in permanent_markers)

    def _retry_control_plane(
        self,
        state: dict[str, Any],
        stage: str,
        operation: Callable[[], Any],
        *,
        keep_retrying_after_grace: bool = True,
    ) -> Any:
        delay = max(1, int(self.config.poll_interval_seconds))
        unreachable_since = state.get("control_plane_unreachable_since")
        if not isinstance(unreachable_since, (int, float)):
            unreachable_since = None

        while not self._closing.is_set():
            try:
                result = operation()
            except Exception as exc:
                if self._is_authentication_error(exc):
                    self._handle_authentication_error(exc)
                if not self._is_retryable_control_plane_error(exc, stage=stage):
                    raise
                if self.config.auth_mode == "device_flow" and not self._is_authentication_error(exc):
                    auth_state = self.client.auth_state()
                    if auth_state.get("status") != "authenticated":
                        self.client.start_device_authentication()
                now = self.now_fn()
                if unreachable_since is None:
                    unreachable_since = now
                    self._append_log(
                        self.runtime._prepare_log_file(str(state["job_id"])),
                        f"[union-worker] Union control plane unavailable during {stage}: {exc}",
                    )
                elapsed = max(0.0, now - float(unreachable_since))
                connectivity = "down" if elapsed >= self.config.unreachable_grace_seconds else "degraded"
                if connectivity == "down" and not keep_retrying_after_grace:
                    raise
                state["control_plane_unreachable_since"] = unreachable_since
                state["control_plane_connectivity"] = connectivity
                state["last_control_plane_error"] = str(exc)
                state["last_control_plane_stage"] = stage

                last_report = state.get("last_control_plane_status_at")
                should_report = not isinstance(last_report, (int, float)) or (
                    now - float(last_report)
                ) >= self.config.status_update_interval_seconds
                if should_report:
                    self.runtime._post_status(
                        str(state["job_id"]),
                        self._active_status(state),
                        details=self._active_details(
                            state,
                            stage=f"union:{stage}:connectivity_{connectivity}",
                            connectivity=connectivity,
                            connectivity_error=str(exc),
                            connectivity_unavailable_seconds=int(elapsed),
                        ),
                    )
                    state["last_control_plane_status_at"] = now
                self._save_state(state)
                self.wait_fn(min(delay, self.config.retry_max_backoff_seconds))
                delay = min(delay * 2, self.config.retry_max_backoff_seconds)
                continue

            self._mark_control_plane_verified(stage)
            if unreachable_since is not None:
                self._append_log(
                    self.runtime._prepare_log_file(str(state["job_id"])),
                    f"[union-worker] Union control plane connectivity recovered during {stage}",
                )
                for key in (
                    "control_plane_unreachable_since",
                    "control_plane_connectivity",
                    "last_control_plane_error",
                    "last_control_plane_stage",
                    "last_control_plane_status_at",
                ):
                    state.pop(key, None)
                self._save_state(state)
            return result

        raise _RecoveryInterrupted(f"Union recovery interrupted during {stage}")

    @staticmethod
    def _append_log(path: Path, message: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(message.rstrip("\n") + "\n")
            handle.flush()

    def _write_progress(self, job_id: str, progress: dict[str, Any]) -> None:
        progress_dir = self._job_dir(job_id) / "progress"
        progress_dir.mkdir(parents=True, exist_ok=True)
        target = progress_dir / "progress.json"
        temporary = progress_dir / ".progress.json.union.tmp"
        payload = dict(progress)
        payload.setdefault("updated_at", self.now_fn())
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.chmod(temporary, 0o666)
        os.replace(temporary, target)
        try:
            os.chmod(progress_dir, 0o777)
            os.chmod(target, 0o666)
        except OSError:
            pass

    def on_startup(self) -> None:
        if self.config.auth_mode == "device_flow":
            self.client.start_device_authentication()
        jobs_root = Path(self.runtime.shared_dir) / "jobs"
        if not jobs_root.is_dir():
            return
        for state_path in jobs_root.glob("*/.worker/union.json"):
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(state, dict) or not state.get("run_name"):
                continue
            job_id = str(state.get("job_id") or state_path.parents[1].name)
            backend_status, backend_exists = self._fetch_backend_status_with_presence(job_id)
            if backend_exists is False:
                self._retire_missing_job_state(state)
                continue
            if backend_status == "recovering" and state.get("terminal_status") == "failed":
                self._reset_recovery_state(state)
            if state.get("terminal") is True and state.get("orchestrator_ack") is True:
                continue
            if backend_status == "queued":
                _LOGGER.info(
                    "Deferring Union recovery for queued job %s until the orchestrator dispatches it",
                    job_id,
                )
                continue
            if not self._claim_job(job_id):
                continue
            job_name = str(state.get("job_name") or job_id)
            job_payload = state.get("job_payload")
            if isinstance(job_payload, dict):
                self.runtime._bind_job_attempt(job_payload)
            if not state.get("gpu_model"):
                state["gpu_model"] = _gpu_model_from_log(self.runtime._prepare_log_file(job_id))
                if state["gpu_model"]:
                    self._save_state(state)
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(
                job_id,
                phase="union:recovery_pending" if state.get("compute_succeeded") else "union:recovering",
                status=self._active_status(state),
                gpu_model=state.get("gpu_model"),
            )
            thread = threading.Thread(
                target=self._recover,
                args=(state,),
                name=f"union-recover-{job_id[:8]}",
                daemon=True,
            )
            self._recovery_threads[job_id] = thread
            thread.start()
            _LOGGER.info("Recovering Union run %s for job %s", state.get("run_name"), job_id)
        self._scan_recovery_requests_if_due(force=True)

    def _reset_recovery_state(self, state: dict[str, Any]) -> None:
        state["terminal"] = False
        state["orchestrator_ack"] = False
        state["recovery_status"] = "requested"
        state["recovery_requested_at"] = self.now_fn()
        for key in ("terminal_status", "terminal_stage", "orchestrator_status", "error", "recovery_error"):
            state.pop(key, None)
        self._save_state(state)

    def _start_requested_recovery(self, job_id: str) -> bool:
        if not job_id:
            return False
        state = self._load_state(job_id)
        if not state or not state.get("run_name"):
            _LOGGER.warning("Union recovery requested for unknown job %s", job_id)
            return False
        if not self._claim_job(job_id):
            _LOGGER.info("Union recovery for job %s is already active", job_id)
            return True
        try:
            self._reset_recovery_state(state)
            job_payload = state.get("job_payload")
            if isinstance(job_payload, dict):
                self.runtime._bind_job_attempt(job_payload)
            job_name = str(state.get("job_name") or job_id)
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(
                job_id,
                phase="union:recovery_requested",
                status="recovering",
                gpu_model=state.get("gpu_model"),
            )
            thread = threading.Thread(
                target=self._recover,
                args=(state,),
                name=f"union-recover-{job_id[:8]}",
                daemon=True,
            )
            self._recovery_threads[job_id] = thread
            thread.start()
            return True
        except Exception:
            self.runtime._unregister_active_job(job_id)
            self._release_job(job_id)
            raise

    def _recover(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        try:
            self._resume_state(state)
        except _RecoveryStopped:
            _LOGGER.info("Union result recovery for job %s was stopped", job_id)
        except _RecoveryInterrupted:
            _LOGGER.info("Union recovery for job %s stopped with the worker", job_id)
        except StaleJobAttemptError:
            _LOGGER.warning("Recovered Union execution attempt for job %s was revoked", job_id)
            self._abort_revoked_run(state)
        except Exception as exc:
            _LOGGER.exception("Failed to recover Union job %s: %s", job_id, exc)
            if state.get("compute_succeeded"):
                try:
                    self._set_recovering(state, "retrying", error=exc)
                except OSError:
                    _LOGGER.exception("Unable to persist deferred recovery state for job %s", job_id)
            else:
                self._finalize_terminal(state, "failed", error=str(exc), stage="union:recovery")
        finally:
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)
            self._release_job(job_id)
            self._recovery_threads.pop(job_id, None)

    def _ensure_input_uploaded(self, state: dict[str, Any]) -> None:
        if state.get("input_uri"):
            return
        job = state.get("job_payload")
        if not isinstance(job, dict):
            raise RuntimeError("Union recovery state is missing the original job payload")
        job_id = str(state["job_id"])
        attempt = max(1, int(state.get("attempt") or 1))
        work_dir = self._job_dir(job_id) / ".worker"
        work_dir.mkdir(parents=True, exist_ok=True)
        archive_path = work_dir / f"union-input-a{attempt}.tar.gz"
        try:
            archived = create_input_archive(
                Path(self.runtime.shared_dir),
                job_id,
                str(job["config_path"]),
                archive_path,
            )
            self._append_log(
                self.runtime._prepare_log_file(job_id),
                f"[union-worker] Input package ready ({len(archived)} roots)",
            )
            self.runtime._post_status(
                job_id,
                "setup",
                details={"executor_stage": "union:uploading_input", "image": job.get("image")},
            )
            input_uri = self._retry_control_plane(
                state,
                "uploading_input",
                lambda: self.client.upload_input(archive_path, job_id, attempt),
            )
            result_uri, cancel_uri = derive_object_uris(str(input_uri))
            state["input_uri"] = str(input_uri)
            state["result_uri"] = result_uri
            state["cancel_uri"] = cancel_uri
            state["input_uploaded"] = True
            self._save_state(state)
        finally:
            archive_path.unlink(missing_ok=True)

    def _resume_state(self, state: dict[str, Any]) -> None:
        if state.get("terminal") is True:
            self._replay_terminal_status(state)
            return
        if state.get("compute_succeeded") is True:
            artifact = state.get("artifact") if isinstance(state.get("artifact"), dict) else None
            self._recover_successful_result(
                state,
                artifact,
                self.runtime._prepare_log_file(str(state["job_id"])),
            )
            return
        if state.get("submitted") is not True and len(str(state.get("run_name") or "")) > 30:
            state["run_name"] = deterministic_run_name(
                str(state["job_id"]),
                max(1, int(state.get("attempt") or 1)),
            )
            state["union_run_id"] = state["run_name"]
            self._save_state(state)
        self._ensure_input_uploaded(state)
        if state.get("submitted") is not True:
            job = state.get("job_payload")
            if not isinstance(job, dict):
                raise RuntimeError("Union recovery state is missing the original job payload")
            self.runtime._post_status(
                str(state["job_id"]),
                "setup",
                details={
                    "executor_stage": "union:submitting",
                    "union_run_id": state.get("run_name"),
                    "image": job.get("image"),
                },
            )
            snapshot = self._retry_control_plane(
                state,
                "submitting",
                lambda: self.client.submit_job(
                    job=job,
                    run_name=str(state["run_name"]),
                    input_uri=str(state["input_uri"]),
                    result_uri=str(state["result_uri"]),
                    cancel_uri=str(state["cancel_uri"]),
                ),
            )
            state["submitted"] = True
            state["run_url"] = snapshot.url
            state["union_phase"] = snapshot.phase
            self._save_state(state)
            self._append_log(
                self.runtime._prepare_log_file(str(state["job_id"])),
                f"[union-worker] Submitted Union run {state['run_name']}",
            )
        self._monitor(state)

    def run_job(self, job: Dict[str, Any]) -> None:
        job_id = str(job["job_id"])
        job_name = str(job.get("job_name") or job_id)
        log_path = self.runtime._prepare_log_file(job_id)
        state: dict[str, Any] | None = None
        if not self._claim_job(job_id):
            self._append_log(
                log_path,
                "[union-worker] Existing recovery already owns this job; ignoring duplicate dispatch",
            )
            return
        try:
            self.runtime._register_active_job(job_id, job_name)
            self.runtime._update_active_job(job_id, phase="union:packaging", status="setup")
            self._append_log(log_path, f"[union-worker] Job accepted: {job_id}")
            self.runtime._post_status(
                job_id,
                "setup",
                details={"executor_stage": "union:packaging", "image": job.get("image")},
            )

            attempt = max(1, int(job.get("attempt_number") or 1))
            existing = self._load_state(job_id)
            existing_attempt = int(existing.get("attempt") or 1) if existing else None
            if existing and existing.get("run_name") and existing_attempt == attempt:
                state = existing
                self._append_log(log_path, f"[union-worker] Resuming existing run {state['run_name']}")
                self._resume_state(state)
                return
            if existing and existing.get("run_name") and existing_attempt != attempt:
                self._append_log(
                    log_path,
                    f"[union-worker] Superseding Union attempt {existing_attempt} with attempt {attempt}",
                )
                if existing.get("terminal") is not True and not self._abort_revoked_run(existing):
                    raise RuntimeError(
                        f"Unable to abort superseded Union run {existing['run_name']}; refusing overlapping attempts"
                    )

            run_name = deterministic_run_name(job_id, attempt)
            state = {
                "schema_version": 2,
                "job_id": job_id,
                "job_name": job_name,
                "worker_id": self.runtime.worker_id,
                "attempt": attempt,
                "run_name": run_name,
                "union_run_id": run_name,
                "image": str(job.get("image") or ""),
                "config_path": str(job["config_path"]),
                "created_at": self.now_fn(),
                "terminal": False,
                "orchestrator_ack": False,
                "last_event_sequence": 0,
                "log_line_count": 0,
                "algorithm_lines_relayed": 0,
                "submitted": False,
                "input_uploaded": False,
                "job_payload": dict(job),
            }
            self._save_state(state)
            self._resume_state(state)
        except _RecoveryStopped:
            _LOGGER.info("Union result recovery for job %s was stopped", job_id)
        except _RecoveryInterrupted:
            _LOGGER.info("Union job %s stopped with the worker and remains recoverable", job_id)
        except StaleJobAttemptError:
            _LOGGER.warning("Union execution attempt for job %s was revoked", job_id)
            if state is not None:
                self._abort_revoked_run(state)
        except Exception as exc:
            _LOGGER.exception("Union job %s failed: %s", job_id, exc)
            self._append_log(log_path, f"[union-worker] Failure: {type(exc).__name__}: {exc}")
            if state is not None:
                if state.get("compute_succeeded"):
                    try:
                        self._set_recovering(state, "retrying", error=exc)
                    except OSError:
                        _LOGGER.exception("Unable to persist deferred recovery state for job %s", job_id)
                else:
                    self._finalize_terminal(state, "failed", error=str(exc), stage="union:failed")
            else:
                self.runtime._post_status(job_id, "failed", error=str(exc), details={"executor_stage": "union:failed"})
        finally:
            self.runtime._unregister_active_job(job_id)
            self.runtime._send_heartbeat(force=True)
            self._release_job(job_id)

    def _terminal_details(self, state: dict[str, Any], status: str, stage: str) -> dict[str, Any]:
        details = {
            "executor_stage": stage,
            "gpu_model": state.get("gpu_model"),
            "union_run_id": state.get("run_name"),
            "union_run_url": state.get("run_url"),
            "union_phase": state.get("union_phase"),
            "terminal_status": status,
            "compute_succeeded": state.get("compute_succeeded") is True,
            "compute_finished_at": state.get("compute_finished_at"),
            "recovery_status": state.get("recovery_status"),
            "recovery_attempts": state.get("recovery_attempts", 0),
        }
        if isinstance(state.get("result_storage"), dict):
            details["result_storage"] = state["result_storage"]
        return details

    def _replay_terminal_status(self, state: dict[str, Any]) -> bool:
        status = str(state.get("terminal_status") or "failed")
        job_id = str(state["job_id"])
        backend_status = self.runtime._fetch_status(job_id)
        if backend_status in _TERMINAL_BACKEND_STATUSES:
            state["orchestrator_ack"] = True
            state["orchestrator_status"] = backend_status
            self._save_state(state)
            return True

        if status == "finished" and backend_status in {"dispatched", "setup"}:
            promoted = self.runtime._post_status(
                job_id,
                "running",
                details=self._active_details(state, stage="union:terminal_recovery"),
            )
            if not promoted:
                return False
        elif status == "stopped" and backend_status in {"dispatched", "setup"}:
            promoted = self.runtime._post_status(
                job_id,
                "stop_requested",
                details=self._active_details(state, stage="union:terminal_recovery"),
            )
            if not promoted:
                return False

        delivered = self.runtime._post_status(
            job_id,
            status,
            exit_code=state.get("runner_exit_code") if isinstance(state.get("runner_exit_code"), int) else None,
            error=state.get("error"),
            details=self._terminal_details(
                state,
                status,
                str(state.get("terminal_stage") or f"union:{status}"),
            ),
        )
        if delivered:
            state["orchestrator_ack"] = True
            state["orchestrator_status"] = status
            self._save_state(state)
            return True
        return False

    def _finalize_terminal(
        self,
        state: dict[str, Any],
        status: str,
        *,
        error: str | None = None,
        stage: str | None = None,
    ) -> None:
        state["terminal"] = True
        state["terminal_status"] = status
        state["terminal_stage"] = stage or f"union:{status}"
        state["orchestrator_ack"] = False
        if error:
            state["error"] = error
        self._save_state(state)
        self._replay_terminal_status(state)

    def _record_started(
        self,
        state: dict[str, Any],
        *,
        started_at: float,
        gpu_model: str | None,
        source: str,
    ) -> None:
        job_id = str(state["job_id"])
        state["started_at"] = started_at
        state["started_at_source"] = source
        if gpu_model:
            state["gpu_model"] = gpu_model
        if str(state.get("requested_terminal_status") or "") in {"stopped", "canceled"}:
            return
        delivered = self.runtime._post_status(
            job_id,
            "running",
            details={
                "executor_stage": "union:running",
                "started_at": started_at,
                "gpu_model": state.get("gpu_model"),
                "union_run_id": state.get("run_name"),
                "union_run_url": state.get("run_url"),
            },
        )
        if delivered:
            self.runtime._send_heartbeat(force=True)

    def _handle_log_line(self, state: dict[str, Any], log_path: Path, line: str) -> None:
        job_id = str(state["job_id"])
        state["log_line_count"] = int(state.get("log_line_count", 0)) + 1
        event = parse_event(line)
        if event is None:
            marker = "[algorithms] "
            cleaned = line.strip("\n")
            if marker in cleaned:
                cleaned = cleaned.split(marker, 1)[1]
                state["algorithm_lines_relayed"] = int(state.get("algorithm_lines_relayed", 0)) + 1
            if cleaned:
                self._append_log(log_path, cleaned)
                if not state.get("gpu_model") and _GPU_MODEL_LOG_MARKER in cleaned:
                    gpu_model = _normalize_gpu_model(cleaned.rsplit(_GPU_MODEL_LOG_MARKER, 1)[1])
                    if gpu_model:
                        state["gpu_model"] = gpu_model
                        self.runtime._update_active_job(job_id, gpu_model=gpu_model)
                        self.runtime._send_heartbeat(force=True)
            if int(state["log_line_count"]) % 25 == 0:
                self._save_state(state)
            return

        sequence = int(event.get("sequence", 0))
        kind = str(event.get("kind"))
        missing_critical_event = (
            (kind == "started" and state.get("started_at_source") != "event")
            or (kind == "artifact" and not isinstance(state.get("artifact"), dict))
            or (kind == "terminal" and not state.get("runner_terminal_status"))
        )
        if sequence <= int(state.get("last_event_sequence", 0)) and not missing_critical_event:
            return
        state["last_event_sequence"] = max(sequence, int(state.get("last_event_sequence", 0)))
        if kind == "setup":
            if isinstance(event.get("cancel_put_url"), str):
                state["cancel_put_url"] = event["cancel_put_url"]
            phase = str(event.get("phase") or "setup")
            state["runner_phase"] = phase
            self.runtime._update_active_job(job_id, phase=f"union:{phase}", status="setup")
        elif kind == "started":
            started_at = float(event.get("started_at") or event.get("timestamp") or self.now_fn())
            gpu_model = _normalize_gpu_model(event.get("gpu_model"))
            self._record_started(state, started_at=started_at, gpu_model=gpu_model, source="event")
        elif kind == "progress" and isinstance(event.get("progress"), dict):
            if not state.get("started_at"):
                self._record_started(
                    state,
                    started_at=float(event.get("timestamp") or self.now_fn()),
                    gpu_model=_normalize_gpu_model(state.get("gpu_model")),
                    source="progress",
                )
            self._write_progress(job_id, dict(event["progress"]))
        elif kind == "artifact" and isinstance(event.get("artifact"), dict):
            state["artifact"] = dict(event["artifact"])
        elif kind == "terminal":
            state["runner_terminal_status"] = str(event.get("status") or "failed")
            state["runner_exit_code"] = event.get("exit_code")
        self._save_state(state)

    def _replay_final_events(self, state: dict[str, Any], log_path: Path) -> None:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                for line in self.client.stream_logs(str(state["run_name"])):
                    value = str(line)
                    if parse_event(value) is not None:
                        self._handle_log_line(state, log_path, value)
                self._mark_control_plane_verified("final_logs")
                return
            except Exception as exc:
                last_error = exc
                # Pod-log authorization is a separate Union/Kubernetes surface.
                # Invalidating the control-plane session here makes healthy Run
                # polling flap while a best-effort log stream is unavailable.
                if attempt < 2:
                    self.wait_fn(2)
        if last_error is not None:
            self._append_log(log_path, f"[union-worker] Final Union events unavailable: {last_error}")

    def _start_log_pump(
        self,
        state: dict[str, Any],
        log_path: Path,
    ) -> tuple[threading.Thread, dict[str, Any], threading.Event]:
        result: dict[str, Any] = {"error": None, "reconnects": 0}
        stop_event = threading.Event()

        def _pump() -> None:
            delay = max(1, int(self.config.poll_interval_seconds))
            max_delay = max(delay, int(self.config.retry_max_backoff_seconds))
            while not self._closing.is_set() and not stop_event.is_set():
                skip_lines = int(state.get("log_line_count", 0))
                received_new_line = False
                result["error"] = None
                try:
                    for index, line in enumerate(self.client.stream_logs(str(state["run_name"]))):
                        if self._closing.is_set() or stop_event.is_set():
                            return
                        if index < skip_lines:
                            continue
                        received_new_line = True
                        self._handle_log_line(state, log_path, str(line))
                except Exception as exc:  # remote logs are best effort; artifacts remain authoritative
                    result["error"] = exc
                    _LOGGER.warning(
                        "Union log stream interrupted for %s; reconnecting: %s",
                        state["job_id"],
                        exc,
                    )

                if self._closing.is_set() or stop_event.is_set():
                    return
                if result["error"] is None:
                    self._mark_control_plane_verified("pod_logs")
                if state.get("runner_terminal_status"):
                    return
                result["reconnects"] = int(result["reconnects"]) + 1
                if received_new_line:
                    delay = max(1, int(self.config.poll_interval_seconds))
                if stop_event.wait(delay):
                    return
                delay = min(delay * 2, max_delay)

        thread = threading.Thread(target=_pump, name=f"union-logs-{str(state['job_id'])[:8]}", daemon=True)
        thread.start()
        return thread, result, stop_event

    def _monitor(self, state: dict[str, Any]) -> None:
        job_id = str(state["job_id"])
        log_path = self.runtime._prepare_log_file(job_id)
        run_name = str(state["run_name"])
        log_thread, log_result, log_stop = self._start_log_pump(state, log_path)
        last_status_update = 0.0
        persisted_cancel_at = state.get("cancel_requested_at")
        cancel_requested_at = (
            float(persisted_cancel_at) if isinstance(persisted_cancel_at, (int, float)) else None
        )
        remote: UnionRunSnapshot | None = None

        while not self._closing.is_set():
            remote = self._retry_control_plane(
                state,
                "polling_run",
                lambda: self.client.get_run(run_name),
            )
            state["union_phase"] = remote.phase
            state["run_url"] = remote.url
            now = self.now_fn()
            backend_status = self.runtime._fetch_status(job_id)
            if backend_status in _STOP_BACKEND_STATUSES:
                requested_terminal_status = "canceled" if backend_status == "canceled" else "stopped"
                state["requested_terminal_status"] = requested_terminal_status
                if cancel_requested_at is None:
                    cancel_requested_at = now
                    state["cancel_requested_at"] = cancel_requested_at
                    self._save_state(state)
                    put_url = str(state.get("cancel_put_url") or "")
                    if put_url and backend_status == "stop_requested":
                        try:
                            self.client.request_graceful_cancel(put_url)
                            state["cancel_signal_sent"] = True
                            self._save_state(state)
                            self._append_log(log_path, "[union-worker] Graceful cancellation requested")
                        except Exception as exc:
                            _LOGGER.warning("Graceful Union cancellation failed: %s", exc)
                    else:
                        self._retry_control_plane(
                            state,
                            "aborting_run",
                            lambda: self.client.abort(run_name, reason=f"OPEVA job {backend_status}"),
                        )
                elif backend_status in {"canceled", "stopped"} or (
                    now - cancel_requested_at
                ) >= self.config.graceful_stop_timeout_seconds:
                    self._retry_control_plane(
                        state,
                        "aborting_run",
                        lambda: self.client.abort(run_name, reason="OPEVA graceful cancellation timeout"),
                    )

            stop_intent = str(state.get("requested_terminal_status") or "") in {"stopped", "canceled"}
            if not stop_intent and now - last_status_update >= self.config.status_update_interval_seconds:
                status = "running" if state.get("started_at") else "setup"
                self.runtime._post_status(
                    job_id,
                    status,
                    details={
                        "executor_stage": "union:running" if status == "running" else "union:provisioning",
                        "started_at": state.get("started_at"),
                        "gpu_model": state.get("gpu_model"),
                        "union_run_id": run_name,
                        "union_run_url": remote.url,
                        "union_phase": remote.phase,
                    },
                )
                last_status_update = now
                self._save_state(state)

            if remote.terminal:
                break
            if backend_status in _TERMINAL_BACKEND_STATUSES:
                break
            self._closing.wait(self.config.poll_interval_seconds)

        if remote is not None and remote.terminal:
            log_thread.join(timeout=15)
        log_stop.set()
        log_thread.join(timeout=1)
        if log_result.get("error"):
            self._append_log(log_path, f"[union-worker] Live logs unavailable: {log_result['error']}")
        if self._closing.is_set() and (remote is None or not remote.terminal):
            self._save_state(state)
            return
        if remote is None:
            raise RuntimeError(f"No Union state was returned for run {run_name}")
        if remote.terminal:
            self._replay_final_events(state, log_path)

        backend_status = self.runtime._fetch_status(job_id)
        if backend_status in _STOP_BACKEND_STATUSES:
            state["requested_terminal_status"] = "canceled" if backend_status == "canceled" else "stopped"
        requested_terminal_status = str(state.get("requested_terminal_status") or "")
        requested_stop = requested_terminal_status in {"stopped", "canceled"}
        artifact = state.get("artifact") if isinstance(state.get("artifact"), dict) else None

        if requested_stop:
            self._delete_stopped_artifact_best_effort(state, artifact, log_path)
            state["union_phase"] = remote.phase
            self._finalize_terminal(state, requested_terminal_status)
            return

        state["union_phase"] = remote.phase
        runner_status = str(state.get("runner_terminal_status") or "")
        if remote.normalized_phase == "SUCCEEDED" and runner_status in {"", "finished"}:
            state["compute_succeeded"] = True
            state.setdefault("compute_finished_at", self.now_fn())
            try:
                self._save_state(state)
            except OSError:
                _LOGGER.exception("Unable to persist compute completion before recovery for job %s", job_id)
            self._recover_successful_result(state, artifact, log_path)
            return

        # Failed compute may still have useful partial logs/results. Recover
        # those best-effort, but preserve the compute failure as terminal.
        if artifact is None and state.get("result_uri"):
            try:
                artifact = self._refresh_artifact(state)
                state["artifact"] = artifact
                self._save_state(state)
            except Exception as exc:
                self._append_log(log_path, f"[union-worker] Partial result artifact unavailable: {exc}")

        if artifact is not None:
            try:
                self._install_artifact(state, artifact, log_path)
            except Exception as exc:
                self._append_log(log_path, f"[union-worker] Partial result recovery failed: {exc}")

        error = f"Union compute ended in {remote.normalized_phase}"
        if runner_status and runner_status != "finished":
            error = f"Union runner ended with status {runner_status}"
        self._finalize_terminal(state, "failed", error=error)

    def _delete_stopped_artifact_best_effort(
        self,
        state: dict[str, Any],
        artifact: dict[str, Any] | None,
        log_path: Path,
    ) -> None:
        if state.get("artifact_deleted") is True:
            return
        try:
            if artifact is None:
                if not state.get("result_uri"):
                    return
                refreshed = self.client.refresh_artifact(str(state["result_uri"]), str(state["job_id"]))
                state["artifact"] = refreshed
                artifact = refreshed
            try:
                self.client.delete_artifact(str(artifact["delete_url"]))
            except Exception:
                refreshed = self.client.refresh_artifact(str(state["result_uri"]), str(state["job_id"]))
                state["artifact"] = refreshed
                self.client.delete_artifact(str(refreshed["delete_url"]))
            state["artifact_deleted"] = True
            state.pop("stopped_artifact_cleanup_error", None)
        except Exception as exc:
            state["stopped_artifact_cleanup_error"] = str(exc)
            self._append_log(
                log_path,
                f"[union-worker] Result artifact cleanup deferred after requested stop: {exc}",
            )
        self._save_state(state)

    def _stop_recovery_if_requested(
        self,
        state: dict[str, Any],
        artifact: dict[str, Any] | None = None,
    ) -> bool:
        backend_status = self.runtime._fetch_status(str(state["job_id"]))
        if backend_status not in _STOP_BACKEND_STATUSES:
            return False
        terminal_status = "canceled" if backend_status == "canceled" else "stopped"
        state["requested_terminal_status"] = terminal_status
        log_path = self.runtime._prepare_log_file(str(state["job_id"]))
        self._append_log(log_path, "[union-worker] Result recovery stopped; skipping result download")
        self._delete_stopped_artifact_best_effort(
            state,
            artifact or (state.get("artifact") if isinstance(state.get("artifact"), dict) else None),
            log_path,
        )
        self._finalize_terminal(state, terminal_status, stage="union:recovery_stopped")
        return True

    def _set_recovering(
        self,
        state: dict[str, Any],
        status: str,
        *,
        error: Exception | str | None = None,
        **details: Any,
    ) -> None:
        job_id = str(state["job_id"])
        state["compute_succeeded"] = True
        state.setdefault("compute_finished_at", self.now_fn())
        state["recovery_status"] = status
        if error is None:
            state.pop("recovery_error", None)
        else:
            state["recovery_error"] = str(error)
        self._save_state(state)
        self.runtime._update_active_job(
            job_id,
            phase=f"union:recovery_{status}",
            status="recovering",
            gpu_model=state.get("gpu_model"),
        )
        self.runtime._post_status(
            job_id,
            "recovering",
            details=self._active_details(
                state,
                stage=f"union:recovery_{status}",
                **details,
            ),
        )

    @contextmanager
    def _recovery_slot(self, state: dict[str, Any]):
        job_id = str(state["job_id"])
        with self._state_lock:
            self._recovery_waiting.add(job_id)
        self._set_recovering(state, "waiting_for_slot")
        acquired = False
        last_stop_check = 0.0
        try:
            while not self._closing.is_set():
                if self._recovery_slots.acquire(timeout=1):
                    acquired = True
                    break
                now = self.now_fn()
                if now - last_stop_check >= self.config.poll_interval_seconds:
                    last_stop_check = now
                    if self._stop_recovery_if_requested(state):
                        raise _RecoveryStopped("Union result recovery was stopped while waiting for a slot")
            if not acquired:
                raise _RecoveryInterrupted("Union recovery interrupted while waiting for a recovery slot")
            with self._state_lock:
                self._recovery_waiting.discard(job_id)
                self._recovery_active.add(job_id)
            yield
        finally:
            with self._state_lock:
                self._recovery_waiting.discard(job_id)
                self._recovery_active.discard(job_id)
            if acquired:
                self._recovery_slots.release()

    def _required_recovery_space(self, artifact: Mapping[str, Any]) -> tuple[int, int, int]:
        archive_size = max(0, int(artifact.get("size") or 0))
        unpacked_size = max(0, int(artifact.get("unpacked_size") or 0))
        if unpacked_size <= 0:
            unpacked_size = archive_size * self.config.recovery_unknown_size_multiplier
        reserve = self.config.recovery_min_free_gib * 1024**3
        return archive_size, unpacked_size, archive_size + unpacked_size + reserve

    def _wait_for_recovery_space(self, state: dict[str, Any], artifact: Mapping[str, Any]) -> None:
        work_dir = self._job_dir(str(state["job_id"])) / ".worker"
        archive_size, unpacked_size, required_free = self._required_recovery_space(artifact)
        last_reported_free: int | None = None
        while not self._closing.is_set():
            if self._stop_recovery_if_requested(state, dict(artifact)):
                raise _RecoveryStopped("Union result recovery was stopped while waiting for local space")
            free = shutil.disk_usage(work_dir).free
            if free >= required_free:
                self._set_recovering(
                    state,
                    "downloading",
                    artifact_size=archive_size,
                    artifact_unpacked_size=unpacked_size,
                    artifact_file_count=max(0, int(artifact.get("file_count") or 0)),
                    recovery_free_bytes=free,
                    recovery_required_free_bytes=required_free,
                )
                return
            if last_reported_free is None or abs(free - last_reported_free) >= 1024**3:
                self._set_recovering(
                    state,
                    "waiting_for_space",
                    error=(
                        f"Insufficient local space: {free} bytes free, "
                        f"{required_free} bytes required"
                    ),
                    artifact_size=archive_size,
                    artifact_unpacked_size=unpacked_size,
                    artifact_file_count=max(0, int(artifact.get("file_count") or 0)),
                    recovery_free_bytes=free,
                    recovery_required_free_bytes=required_free,
                )
                last_reported_free = free
            self.wait_fn(self.config.recovery_retry_interval_seconds)
        raise _RecoveryInterrupted("Union recovery interrupted while waiting for local space")

    def _recover_successful_result(
        self,
        state: dict[str, Any],
        artifact: dict[str, Any] | None,
        log_path: Path,
    ) -> None:
        delay = self.config.recovery_retry_interval_seconds
        max_delay = max(delay, self.config.retry_max_backoff_seconds * 5)
        while not self._closing.is_set():
            try:
                if self._stop_recovery_if_requested(state, artifact):
                    return
                self._set_recovering(state, "pending")
                with self._recovery_slot(state):
                    if self._stop_recovery_if_requested(state, artifact):
                        return
                    if artifact is None or state.get("artifact_installed") is not True:
                        artifact = self._refresh_artifact(state) if artifact is None else artifact
                        state["artifact"] = artifact
                        self._save_state(state)
                    if state.get("artifact_installed") is not True:
                        self._wait_for_recovery_space(state, artifact)
                    else:
                        self._set_recovering(state, "cleaning_remote_artifact")
                    self._install_artifact(state, artifact, log_path)
                state["recovery_status"] = "complete"
                state.pop("recovery_error", None)
                self._save_state(state)
                self._finalize_terminal(state, "finished", stage="union:finished")
                return
            except (_RecoveryInterrupted, _RecoveryStopped, StaleJobAttemptError):
                raise
            except Exception as exc:
                state["recovery_attempts"] = int(state.get("recovery_attempts", 0)) + 1
                try:
                    self._set_recovering(state, "retrying", error=exc)
                except OSError:
                    _LOGGER.exception("Unable to persist Union recovery failure for job %s", state["job_id"])
                self._append_log(
                    log_path,
                    f"[union-worker] Result recovery deferred; retrying without rerunning compute: {exc}",
                )
                self.wait_fn(delay)
                delay = min(delay * 2, max_delay)
        raise _RecoveryInterrupted("Union recovery interrupted before result installation")

    def _refresh_artifact(self, state: dict[str, Any]) -> dict[str, Any]:
        job_id = str(state["job_id"])
        result_uri = str(state["result_uri"])
        delay = max(1, int(self.config.poll_interval_seconds))
        max_delay = max(delay, int(self.config.retry_max_backoff_seconds))
        attempts = max(1, int(self.config.artifact_refresh_attempts))
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            if self._closing.is_set():
                raise _RecoveryInterrupted("Union recovery interrupted while waiting for the result artifact")
            try:
                artifact = dict(self.client.refresh_artifact(result_uri, job_id))
                self._mark_control_plane_verified("refreshing_artifact")
                return artifact
            except Exception as exc:
                last_error = exc
                if self._is_authentication_error(exc):
                    self._handle_authentication_error(exc)
                if not self._is_retryable_control_plane_error(exc, stage="refreshing_artifact"):
                    raise
                if self.config.auth_mode == "device_flow" and not self._is_authentication_error(exc):
                    auth_state = self.client.auth_state()
                    if auth_state.get("status") != "authenticated":
                        self.client.start_device_authentication()
                if attempt >= attempts:
                    break
                self._append_log(
                    self.runtime._prepare_log_file(job_id),
                    f"[union-worker] Result artifact not available yet; retrying ({attempt}/{attempts}): {exc}",
                )
                self.runtime._post_status(
                    job_id,
                    self._active_status(state),
                    details=self._active_details(state, stage="union:waiting_for_artifact"),
                )
                self.wait_fn(delay)
                delay = min(delay * 2, max_delay)

        raise RuntimeError(
            f"Union result artifact remained unavailable after {attempts} attempts: {last_error}"
        ) from last_error

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _install_artifact(self, state: dict[str, Any], artifact: dict[str, Any], log_path: Path) -> None:
        job_id = str(state["job_id"])
        work_dir = self._job_dir(job_id) / ".worker"
        work_dir.mkdir(parents=True, exist_ok=True)
        archive_path = work_dir / "union-result.tar.gz"
        extracted: Path | None = None
        try:
            if self._stop_recovery_if_requested(state, artifact):
                raise _RecoveryStopped("Union result recovery was stopped before download")
            if state.get("artifact_installed") is not True:
                extracted = Path(tempfile.mkdtemp(prefix=f"opeva-union-{job_id[:8]}-", dir=str(work_dir)))

                def _download() -> None:
                    nonlocal artifact
                    try:
                        self.client.download_artifact(str(artifact["get_url"]), archive_path)
                    except Exception:
                        artifact = self.client.refresh_artifact(str(state["result_uri"]), job_id)
                        state["artifact"] = artifact
                        self._save_state(state)
                        self.client.download_artifact(str(artifact["get_url"]), archive_path)

                self._retry_control_plane(state, "downloading_artifact", _download)
                if self._stop_recovery_if_requested(state, artifact):
                    raise _RecoveryStopped("Union result recovery was stopped after download")

                expected_size = int(artifact.get("size") or 0)
                expected_sha = str(artifact.get("sha256") or "")
                if expected_size <= 0 or archive_path.stat().st_size != expected_size:
                    raise RuntimeError(
                        f"Union artifact size mismatch: expected {expected_size}, got {archive_path.stat().st_size}"
                    )
                actual_sha = self._file_sha256(archive_path)
                if len(expected_sha) != 64 or actual_sha != expected_sha:
                    raise RuntimeError(f"Union artifact checksum mismatch: expected {expected_sha}, got {actual_sha}")
                if state.get("compute_succeeded"):
                    self._set_recovering(state, "extracting")
                safe_extract(archive_path, extracted)
                if self._stop_recovery_if_requested(state, artifact):
                    raise _RecoveryStopped("Union result recovery was stopped after extraction")
                state["algorithm_lines_relayed"] = append_missing_algorithm_logs(
                    log_path,
                    extracted,
                    job_id,
                    int(state.get("algorithm_lines_relayed", 0)),
                )
                if state.get("compute_succeeded"):
                    self._set_recovering(state, "installing")
                merge_job_results(extracted, Path(self.runtime.shared_dir), job_id)
                state["result_storage"] = write_result_storage_manifest(
                    self._job_dir(job_id),
                    transferred_bytes=int(artifact.get("size") or archive_path.stat().st_size),
                    announced_unpacked_bytes=int(artifact.get("unpacked_size") or 0),
                    announced_file_count=int(artifact.get("file_count") or 0),
                )
                state["artifact_installed"] = True
                self._save_state(state)

            if state.get("artifact_installed") is True and not isinstance(state.get("result_storage"), dict):
                state["result_storage"] = write_result_storage_manifest(
                    self._job_dir(job_id),
                    transferred_bytes=int(artifact.get("size") or 0),
                    announced_unpacked_bytes=int(artifact.get("unpacked_size") or 0),
                    announced_file_count=int(artifact.get("file_count") or 0),
                )
                self._save_state(state)

            if state.get("artifact_deleted") is not True:
                if state.get("compute_succeeded"):
                    self._set_recovering(state, "cleaning_remote_artifact")
                def _delete() -> None:
                    nonlocal artifact
                    try:
                        self.client.delete_artifact(str(artifact["delete_url"]))
                    except Exception:
                        artifact = self.client.refresh_artifact(str(state["result_uri"]), job_id)
                        state["artifact"] = artifact
                        self._save_state(state)
                        self.client.delete_artifact(str(artifact["delete_url"]))

                self._retry_control_plane(state, "deleting_artifact", _delete)
                state["artifact_deleted"] = True
                self._save_state(state)
        finally:
            archive_path.unlink(missing_ok=True)
            if extracted is not None:
                shutil.rmtree(extracted, ignore_errors=True)

    def close(self) -> None:
        self._closing.set()
        for thread in list(self._recovery_threads.values()):
            thread.join(timeout=5)
